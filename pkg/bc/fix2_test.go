package bc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// --- helpers

func setVar[T any](t *testing.T, v *T, val T) {
	old := *v
	*v = val
	t.Cleanup(func() { *v = old })
}

func fastProbe(t *testing.T) { setVar(t, &probeVideoTimeout, 100*time.Millisecond) }

func swanFrames(t testing.TB) []*Media {
	var d Demuxer
	d.Push(swan(t))
	return drain(&d)
}

func frameBytes(t testing.TB, ms ...*Media) []byte {
	var out []byte
	for _, m := range ms {
		b, err := m.Marshal()
		require.NoError(t, err)
		out = append(out, b...)
	}
	return out
}

// annexB joins NAL units with 4 byte start codes.
func annexB(nals ...[]byte) []byte {
	var out []byte
	for _, n := range nals {
		out = append(out, 0, 0, 0, 1)
		out = append(out, n...)
	}
	return out
}

func nalsOf(t testing.TB, m *Media, hevc bool) [][]byte {
	nals, ok := splitAVCC(annexb.EncodeToAVCC(m.Data), hevc)
	require.True(t, ok)
	var out [][]byte
	for _, n := range nals {
		out = append(out, n.data)
	}
	return out
}

// adts makes one ADTS frame: AAC-LC, 16 kHz, mono, one block.
func adts(payload []byte) []byte {
	size := 7 + len(payload)
	h := []byte{0xFF, 0xF1, 0x60, 0x40 | byte(size>>11)&3, byte(size >> 3), byte(size<<5) | 0x1F, 0xFC}
	return append(h, payload...)
}

func newFakeProducer(t *testing.T, f *fakeCamera, cfg Config) *Producer {
	setVar(t, &probeAudioTimeout, 100*time.Millisecond) // streams without audio
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	p, err := NewProducer(conn, cfg, StreamMain, "bc://test")
	require.NoError(t, err)
	t.Cleanup(func() { p.Stop() })
	return p
}

var (
	h265VPS = []byte{0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x01, 0x60, 0, 0, 3, 0, 0x90}
	h265SPS = []byte{0x42, 0x01, 0x01, 0x01, 0x60, 0, 0, 3, 0, 0x90, 0, 0, 3, 0, 0, 0x3c, 0xa0, 0x0a, 0x08, 0x0f, 0x16, 0x59}
	h265PPS = []byte{0x44, 0x01, 0xc1, 0x72, 0xb4, 0x62, 0x40}
	h265IDR = []byte{0x26, 0x01, 0xaf, 0x08, 0x42, 0x23, 0x90, 0x11, 0x22, 0x33}
	h265TRA = []byte{0x02, 0x01, 0xd0, 0x09, 0x7e, 0x10, 0x20, 0x30}
)

func h265Key(us uint32) *Media {
	return &Media{Kind: KindIframe, Codec: "H265", Microseconds: us, Data: annexB(h265VPS, h265SPS, h265PPS, h265IDR)}
}

func h265P(us uint32) *Media {
	return &Media{Kind: KindPframe, Codec: "H265", Microseconds: us, Data: annexB(h265TRA)}
}

type packets struct {
	mu   sync.Mutex
	pkts []*rtp.Packet
}

func (r *packets) add(p *rtp.Packet) {
	r.mu.Lock()
	r.pkts = append(r.pkts, p)
	r.mu.Unlock()
}

func (r *packets) all() []*rtp.Packet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*rtp.Packet(nil), r.pkts...)
}

// --- F1: malformed video never panics

func TestProbeMalformedVideo(t *testing.T) {
	fastProbe(t)

	short := func(b ...byte) []byte { return b }
	tests := map[string]*Media{
		// the reviewer's case: every NAL is 2 bytes, the SPS has no profile
		"h264 tiny nals": {Kind: KindIframe, Codec: "H264", Data: []byte{
			0, 0, 1, 0x41, 0x01, 0, 0, 1, 0x06, 0x01, 0, 0, 1, 0x68, 0x01, 0, 0, 1, 0x67, 0x01}},
		"h264 short sps": {Kind: KindIframe, Codec: "H264", Data: annexB(short(0x67, 1, 2), short(0x68, 0xee), short(0x65, 1, 2, 3))},
		"h264 short pps": {Kind: KindIframe, Codec: "H264", Data: annexB(short(0x67, 1, 2, 3, 4), short(0x68), short(0x65, 1, 2, 3))},
		"h264 no slice":  {Kind: KindIframe, Codec: "H264", Data: annexB(short(0x67, 1, 2, 3, 4), short(0x68, 0xee))},
		"h264 empty":     {Kind: KindIframe, Codec: "H264", Data: nil},
		"h264 garbage":   {Kind: KindIframe, Codec: "H264", Data: []byte{0, 0, 1}},
		"h265 tiny":      {Kind: KindIframe, Codec: "H265", Data: annexB(short(0x40, 1), short(0x42, 1), short(0x44, 1), short(0x26, 1))},
		"h265 one byte":  {Kind: KindIframe, Codec: "H265", Data: annexB(short(0x40), short(0x42), short(0x44), short(0x26))},
		"h265 no pps":    {Kind: KindIframe, Codec: "H265", Data: annexB(h265VPS, h265SPS, h265IDR)},
		"h265 empty":     {Kind: KindIframe, Codec: "H265", Data: nil},
	}
	for name, m := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFakeCamera(t)
			f.stream = frameBytes(t, m)
			conn, err := DialTCP(f.addr())
			require.NoError(t, err)
			_, err = NewProducer(conn, fastConfig(), StreamMain, "bc://test")
			require.ErrorContains(t, err, "no keyframe")
		})
	}
}

func TestVideoConsumersSurviveMalformedFrames(t *testing.T) {
	bad := func(codec string, data []byte, kind MediaKind) *Media {
		return &Media{Kind: kind, Codec: codec, Data: data}
	}

	t.Run("h264", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = swan(t)
		p := newFakeProducer(t, f, fastConfig())

		var out packets
		repair := h264.RepairAVCC(p.video.Codec, out.add)
		p.video.Input = func(pkt *rtp.Packet) { repair(pkt) } // what WebRTC and MP4 do

		frames := swanFrames(t)
		key, pf := frames[0], frames[1]
		sps := nalsOf(t, key, false)

		for name, m := range map[string]*Media{
			"empty p":    bad("H264", nil, KindPframe),
			"empty i":    bad("H264", nil, KindIframe),
			"one byte":   bad("H264", []byte{0}, KindPframe),
			"sei only":   bad("H264", annexB([]byte{0x06, 0x05, 0x01}), KindPframe),
			"sei only i": bad("H264", annexB([]byte{0x06, 0x05, 0x01}), KindIframe),
			"no vcl":     bad("H264", annexB(sps[0], sps[1]), KindIframe),
			"no data":    bad("H264", []byte{0, 0, 0, 1}, KindPframe),
		} {
			useful, err := p.handle(m)
			require.NoError(t, err, name)
			require.False(t, useful, name)
		}
		require.Empty(t, out.all(), "nothing malformed reached the consumer")

		// held back until a good keyframe, a P-frame is not enough
		useful, err := p.handle(pf)
		require.NoError(t, err)
		require.False(t, useful)
		require.Empty(t, out.all())

		useful, err = p.handle(key)
		require.NoError(t, err)
		require.True(t, useful)
		useful, err = p.handle(pf)
		require.NoError(t, err)
		require.True(t, useful)
		require.Len(t, out.all(), 2)
	})

	t.Run("h265", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = frameBytes(t, h265Key(0))
		p := newFakeProducer(t, f, fastConfig())
		require.Equal(t, core.CodecH265, p.video.Codec.Name)

		var out packets
		repair := h265.RepairAVCC(p.video.Codec, out.add)
		p.video.Input = func(pkt *rtp.Packet) { repair(pkt) }

		for name, m := range map[string]*Media{
			"empty":    bad("H265", nil, KindPframe),
			"one byte": bad("H265", annexB([]byte{0x02}), KindPframe),
			"short":    bad("H265", []byte{0, 0, 1, 0x02}, KindPframe),
			"no vcl":   bad("H265", annexB(h265VPS, h265SPS, h265PPS), KindIframe),
		} {
			useful, err := p.handle(m)
			require.NoError(t, err, name)
			require.False(t, useful, name)
		}
		require.Empty(t, out.all())

		useful, err := p.handle(h265P(40000))
		require.NoError(t, err)
		require.False(t, useful, "held back until a good keyframe")

		useful, err = p.handle(h265Key(80000))
		require.NoError(t, err)
		require.True(t, useful)
		useful, err = p.handle(h265P(120000))
		require.NoError(t, err)
		require.True(t, useful)
		require.Len(t, out.all(), 2)
	})
}

// --- F6: format changes end the producer

func TestFormatChangeEndsStart(t *testing.T) {
	t.Run("parameter sets", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = swan(t)
		p := newFakeProducer(t, f, fastConfig())
		key := swanFrames(t)[0]

		// the same keyframe is fine, also without its parameter sets
		useful, err := p.handle(key)
		require.NoError(t, err)
		require.True(t, useful)
		nals := nalsOf(t, key, false)
		useful, err = p.handle(&Media{Kind: KindIframe, Codec: "H264", Data: annexB(nals[2:]...)})
		require.NoError(t, err)
		require.True(t, useful)

		// a new SPS (resolution or profile change)
		changed := append([]byte(nil), nals[0]...)
		changed[len(changed)-1] ^= 0x55
		_, err = p.handle(&Media{Kind: KindIframe, Codec: "H264", Data: annexB(append([][]byte{changed}, nals[1:]...)...)})
		require.ErrorIs(t, err, ErrFormatChanged)

		// and a new PPS
		changed = append([]byte(nil), nals[1]...)
		changed[len(changed)-1] ^= 0x55
		_, err = p.handle(&Media{Kind: KindIframe, Codec: "H264", Data: annexB(append([][]byte{nals[0], changed}, nals[2:]...)...)})
		require.ErrorIs(t, err, ErrFormatChanged)
	})

	t.Run("h265 parameter sets", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = frameBytes(t, h265Key(0))
		p := newFakeProducer(t, f, fastConfig())

		for i, ps := range [][]byte{h265VPS, h265SPS, h265PPS} {
			changed := append([]byte(nil), ps...)
			changed[len(changed)-1] ^= 0x55
			sets := [][]byte{h265VPS, h265SPS, h265PPS}
			sets[i] = changed
			_, err := p.handle(&Media{Kind: KindIframe, Codec: "H265", Data: annexB(append(sets, h265IDR)...)})
			require.ErrorIs(t, err, ErrFormatChanged, "parameter set %d", i)
		}
	})

	t.Run("codec", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = swan(t)
		p := newFakeProducer(t, f, fastConfig())
		_, err := p.handle(h265Key(0))
		require.ErrorIs(t, err, ErrFormatChanged)
		_, err = p.handle(h265P(0))
		require.ErrorIs(t, err, ErrFormatChanged)

		f2 := newFakeCamera(t)
		f2.stream = frameBytes(t, h265Key(0))
		p2 := newFakeProducer(t, f2, fastConfig())
		_, err = p2.handle(swanFrames(t)[0])
		require.ErrorIs(t, err, ErrFormatChanged)
	})

	t.Run("audio", func(t *testing.T) {
		f := newFakeCamera(t) // swan: ADPCM
		f.stream = swan(t)
		p := newFakeProducer(t, f, fastConfig())
		require.Equal(t, core.CodecPCMA, p.audio.Codec.Name)

		aacFrame := &Media{Kind: KindAAC, Data: adts([]byte{1, 2, 3, 4}), AudioRate: 16000}
		_, err := p.handle(aacFrame)
		require.ErrorContains(t, err, "audio format changed")

		// AAC stream whose ADTS format changes (here: another sample rate)
		video := swanFrames(t)[0]
		f2 := newFakeCamera(t)
		f2.stream = frameBytes(t, video, &Media{Kind: KindAAC, Data: adts([]byte{1, 2, 3, 4})})
		p2 := newFakeProducer(t, f2, fastConfig())
		require.Equal(t, core.CodecAAC, p2.audio.Codec.Name)
		other := adts([]byte{1, 2, 3, 4})
		other[2] = other[2]&^0x3c | 3<<2 // 48 kHz
		_, err = p2.handle(&Media{Kind: KindAAC, Data: other, AudioRate: 48000})
		require.ErrorContains(t, err, "audio format changed")
		_, err = p2.handle(&Media{Kind: KindADPCM, Data: make([]byte, 20), AudioRate: 8000})
		require.ErrorContains(t, err, "audio format changed")
	})
}

func TestIgnoredFramesDoNotFeedWatchdog(t *testing.T) {
	setVar(t, &idleTimeout, 300*time.Millisecond)
	f := newFakeCamera(t)
	f.stream = swan(t)
	p := newFakeProducer(t, f, fastConfig())

	errc := make(chan error, 1)
	go func() { errc <- p.Start() }()

	// after the swan frames only things we cannot use arrive, forever
	info := frameBytes(t, &Media{Kind: KindInfoV1})
	damaged := frameBytes(t, &Media{Kind: KindPframe, Codec: "H264", Data: []byte{0}})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
				f.push(info)
				f.push(damaged)
			}
		}
	}()

	select {
	case err := <-errc:
		require.ErrorContains(t, err, "no usable media")
	case <-time.After(3 * time.Second):
		t.Fatal("unusable frames kept the producer alive")
	}
}

// --- F5: Start closes the camera session on every return

func TestStartClosesClientOnIdle(t *testing.T) {
	setVar(t, &idleTimeout, 300*time.Millisecond)
	f := newFakeCamera(t) // answers pings, sends the swan once, then nothing
	f.stream = swan(t)
	p := newFakeProducer(t, f, fastConfig())

	err := p.Start()
	require.ErrorContains(t, err, "no usable media")
	select {
	case <-p.client.Done():
	default:
		t.Fatal("client still open after Start returned")
	}

	// the camera side sees the connection go
	f.mu.Lock()
	conn := f.conn.(net.Conn)
	f.mu.Unlock()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	require.NotErrorIs(t, err, net.ErrClosed)
	var ne net.Error
	require.False(t, errors.As(err, &ne) && ne.Timeout(), "camera connection still open")

	// but the receivers are intact for go2rtc's replacement
	p.video.WriteRTP(&rtp.Packet{Payload: []byte{0}})
}

func TestStartClosesClientOnFormatChange(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	p := newFakeProducer(t, f, fastConfig())
	errc := make(chan error, 1)
	go func() { errc <- p.Start() }()

	f.push(frameBytes(t, h265Key(0)))
	select {
	case err := <-errc:
		require.ErrorIs(t, err, ErrFormatChanged)
	case <-time.After(3 * time.Second):
		t.Fatal("format change not reported")
	}
	<-p.client.Done()
}

// --- F3: the stream clocks advance for dropped frames

func TestStreamClocksSurviveDrops(t *testing.T) {
	const frames = 400
	var stream []byte
	var aacIdx int
	for i := 0; i < frames; i++ {
		kind := KindPframe
		if i%20 == 0 {
			kind = KindIframe
		}
		var payload [4]byte
		binary.BigEndian.PutUint32(payload[:], uint32(i))
		stream = append(stream, frameBytes(t, &Media{
			Kind: kind, Codec: "H264", Microseconds: uint32(1000 + i*40000),
			Data: append([]byte{0, 0, 0, 1, 0x41}, payload[:]...),
		})...)
		if i%8 == 0 { // five 64 ms AAC frames every 320 ms
			for k := 0; k < 5; k++ {
				var idx [4]byte
				binary.BigEndian.PutUint32(idx[:], uint32(aacIdx))
				stream = append(stream, frameBytes(t, &Media{Kind: KindAAC, Data: adts(idx[:])})...)
				aacIdx++
			}
		}
	}

	f := newFakeCamera(t)
	f.stream = stream
	cfg := fastConfig()
	cfg.StreamBuffer = 4
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)
	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	// nobody reads while the camera sends everything: most frames are dropped
	require.Eventually(t, func() bool { return st.Dropped() > 300 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	var video, audio int
	for len(st.C) > 0 {
		m := <-st.C
		idx := int(binary.BigEndian.Uint32(m.Data[len(m.Data)-4:]))
		switch m.Kind {
		case KindIframe, KindPframe:
			video++
			require.Equal(t, uint64(idx)*40000, m.PTS, "video frame %d", idx)
		case KindAAC:
			audio++
			// anchored on the video clock at the first audio frame (video frame 0, so 0)
			require.Equal(t, uint32(16000), m.AudioRate)
			require.Equal(t, uint64(idx)*1024, m.AudioPos, "audio frame %d", idx)
		}
	}
	require.NotZero(t, video)
	require.NotZero(t, audio)
	require.Less(t, video+audio, frames+aacIdx, "frames were dropped")
}

func TestAudioClockAnchorsOnVideo(t *testing.T) {
	var a audioClock
	// first audio after 2.5 s of video, 16 kHz
	require.Equal(t, uint64(40000), a.next(16000, 1024, 2_500_000))
	require.Equal(t, uint64(41024), a.next(16000, 1024, 3_000_000), "no re-anchoring")
	// another sample rate starts a new timeline
	require.Equal(t, uint64(8000*3), a.next(8000, 480, 3_000_000))
}

func TestParseADTS(t *testing.T) {
	two := append(adts([]byte{1, 2, 3}), adts([]byte{4, 5})...)
	info, ok := parseADTS(append(two, 0xAA, 0xBB)) // trailing garbage is ignored
	require.True(t, ok)
	require.Equal(t, uint32(16000), info.rate)
	require.Equal(t, uint64(2048), info.samples)
	require.Len(t, info.frames, 2)
	require.Equal(t, adtsFrame{header: 7, size: 10, blocks: 1}, info.frames[0])

	for name, b := range map[string][]byte{
		"empty":     nil,
		"no sync":   {1, 2, 3, 4, 5, 6, 7, 8},
		"too short": adts(nil)[:6],
		"bad size":  append(adts([]byte{1})[:3], 0xff, 0xe0, 0xfc, 0),
	} {
		_, ok = parseADTS(b)
		require.False(t, ok, name)
	}
	bad := adts([]byte{1, 2})
	bad[2] |= 0x3c // sampling index 15
	_, ok = parseADTS(bad)
	require.False(t, ok)
}

// --- F4: AAC leaves as standard RTP

func TestAACIsStandardRTP(t *testing.T) {
	video := swanFrames(t)[0]
	au := []byte{0x21, 0x10, 0x04, 0x60, 0x8c, 0x1c}
	f := newFakeCamera(t)
	f.stream = frameBytes(t, video, &Media{Kind: KindAAC, Data: adts(au)})
	p := newFakeProducer(t, f, fastConfig())

	codec := p.audio.Codec
	want := aac.ADTSToCodec(adts(au))
	require.Equal(t, core.CodecAAC, codec.Name)
	require.Equal(t, uint32(16000), codec.ClockRate)
	require.Equal(t, want.FmtpLine, codec.FmtpLine, "the fmtp RTSP cameras have")
	require.True(t, codec.IsRTP(), "RTP packets, so the RTSP consumer does not run aac.RTPPay over them")

	var raw, depayed packets
	depay := aac.RTPDepay(depayed.add) // what the MP4, FLV and TS consumers put in front
	p.audio.Input = func(pkt *rtp.Packet) { raw.add(pkt); depay(pkt) }

	// two frames in one BcMedia frame, then a gap of three dropped frames
	two := &Media{Kind: KindAAC, Data: append(adts(au), adts(au[:3])...), AudioRate: 16000, AudioPos: 160000}
	useful, err := p.handle(two)
	require.NoError(t, err)
	require.True(t, useful)
	next := &Media{Kind: KindAAC, Data: adts(au), AudioRate: 16000, AudioPos: 160000 + 5*1024}
	_, err = p.handle(next)
	require.NoError(t, err)

	pk := raw.all()
	require.Len(t, pk, 3)
	for _, x := range pk {
		require.Equal(t, uint8(2), x.Version)
		require.True(t, x.Marker)
		require.Equal(t, uint8(aacPayloadType), x.PayloadType)
	}
	// AU headers section: 16 bits long, one header with the size in 13 bits
	require.Equal(t, []byte{0, 16, 0, byte(len(au)) << 3}, pk[0].Payload[:4])
	require.Equal(t, au, pk[0].Payload[4:])
	require.Equal(t, au[:3], pk[1].Payload[4:])

	// the timeline of the stream, gaps included, is what is on the wire
	require.Equal(t, pk[0].Timestamp+1024, pk[1].Timestamp)
	require.Equal(t, pk[1].Timestamp+1024+3*1024, pk[2].Timestamp, "dropped duration is a gap")
	require.Equal(t, pk[0].SequenceNumber+1, pk[1].SequenceNumber)

	// standard depacketizers get the raw access units back
	got := depayed.all()
	require.Len(t, got, 3)
	require.Equal(t, au, got[0].Payload)
	require.Equal(t, au[:3], got[1].Payload)
}

// --- F7: the timeline continues across producers

func TestTimelineContinuesAcrossProducers(t *testing.T) {
	run := func() (video, audio []*rtp.Packet) {
		f := newFakeCamera(t)
		f.stream = swan(t)
		p := newFakeProducer(t, f, fastConfig())
		var v, a packets
		p.video.Input = v.add
		p.audio.Input = a.add
		errc := make(chan error, 1)
		go func() { errc <- p.Start() }()
		require.Eventually(t, func() bool { return len(v.all()) == 3 && len(a.all()) == 2 }, 3*time.Second, 5*time.Millisecond)
		require.NoError(t, p.Stop())
		<-errc
		return v.all(), a.all()
	}

	v1, a1 := run()
	v2, a2 := run()

	// the second camera clock is unrelated (same samples, same values), its
	// timestamps still continue after the first producer's
	require.Greater(t, v2[0].Timestamp, v1[len(v1)-1].Timestamp)
	require.Greater(t, a2[0].Timestamp, a1[len(a1)-1].Timestamp)
	// inside one producer the timestamps come from the camera clock, not arrival
	require.Equal(t, v1[1].Timestamp-v1[0].Timestamp, v2[1].Timestamp-v2[0].Timestamp)
	require.Equal(t, uint32(480), a1[1].Timestamp-a1[0].Timestamp)

	// even if the process clock is behind what a camera clock produced
	timelineEnd.Store(timelineEnd.Load() + 3_600_000_000) // one hour ahead
	end := timelineEnd.Load()
	v3, _ := run()
	require.Greater(t, uint64(v3[0].Timestamp), end*9/100)
}

// --- F8: login encryption

func TestLoginMaxEncryption(t *testing.T) {
	tests := []struct {
		name string
		enc  MaxEncryption
		code uint16
		mode Mode
	}{
		{"default", EncryptionDefault, 0xdc12, ModeAES},
		{"aes", EncryptionAES, 0xdc12, ModeAES},
		{"bc", EncryptionBC, 0xdc01, ModeBC},
		{"none", EncryptionNone, 0xdc00, ModeNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeCamera(t)
			f.stream = swan(t)
			cfg := fastConfig()
			cfg.Encryption = tc.enc
			c := dialFake(t, f, cfg)
			_, err := c.Login()
			require.NoError(t, err)

			f.mu.Lock()
			require.Equal(t, []uint16{tc.code}, f.loginCodes)
			f.mu.Unlock()
			require.Equal(t, tc.mode, c.codec.Cipher().Mode)

			// the negotiated level carries a real exchange
			st, err := c.StartStream(StreamMain)
			require.NoError(t, err)
			require.NotNil(t, <-st.C)
		})
	}
}

func TestParseMaxEncryption(t *testing.T) {
	for s, want := range map[string]MaxEncryption{
		"": EncryptionDefault, "aes": EncryptionAES, "bc": EncryptionBC, "none": EncryptionNone,
	} {
		got, ok := ParseMaxEncryption(s)
		require.True(t, ok)
		require.Equal(t, want, got)
	}
	_, ok := ParseMaxEncryption("rot13")
	require.False(t, ok)

	_, err := Dial("bc://u:p@127.0.0.1?encryption=rot13")
	require.ErrorContains(t, err, "unknown encryption")
}

// --- F9: msg_num allocation

func TestNumAllocationSkipsReserved(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.repeat = 1
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)
	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)
	require.NotNil(t, <-st.C)
	require.True(t, c.codec.isBinary(st.num), "stream runs in binary mode")

	// the counter is about to hand out the stream's number
	c.num.Store(uint32(st.num) - 1)
	w1, err := c.subscribeNew(MsgPing)
	require.NoError(t, err)
	defer w1.close()
	require.NotEqual(t, st.num, w1.num)

	// and now the waiter's one
	c.num.Store(uint32(w1.num) - 1)
	w2, err := c.subscribeNew(MsgPing)
	require.NoError(t, err)
	defer w2.close()
	require.NotEqual(t, w1.num, w2.num)
	require.NotEqual(t, st.num, w2.num)

	// a real ping on the colliding number still works and is a normal reply
	c.num.Store(uint32(st.num) - 1)
	require.NoError(t, c.ping())

	// zero is never used
	c.num.Store(0xffff)
	w3, err := c.subscribeNew(MsgPing)
	require.NoError(t, err)
	defer w3.close()
	require.NotZero(t, w3.num)

	// when the stream ends its binary mode goes with it
	require.NoError(t, st.Stop(time.Second))
	require.False(t, c.codec.isBinary(st.num))
	c.num.Store(uint32(st.num) - 1)
	w4, err := c.subscribeNew(MsgPing)
	require.NoError(t, err)
	defer w4.close()
	require.Equal(t, st.num, w4.num, "free again")
}

func TestWaiterMatchesMsgID(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	w, err := c.subscribeNew(MsgPing)
	require.NoError(t, err)
	defer w.close()

	c.dispatch(&Message{MsgID: MsgTalk, MsgNum: w.num, ResponseCode: 200})
	c.dispatch(&Message{MsgID: MsgVideo, MsgNum: w.num, Binary: []byte{1, 2, 3}})
	select {
	case m := <-w.ch:
		t.Fatalf("delivered a reply of message %d to a ping waiter", m.MsgID)
	default:
	}
	c.dispatch(&Message{MsgID: MsgPing, MsgNum: w.num, ResponseCode: 200})
	require.Len(t, w.ch, 1)
}

// --- F10: IPv6

func TestTCPAddr(t *testing.T) {
	for in, want := range map[string]string{
		"192.168.1.2":       "192.168.1.2:9000",
		"192.168.1.2:9001":  "192.168.1.2:9001",
		"cam.local":         "cam.local:9000",
		"cam.local:1234":    "cam.local:1234",
		"[::1]":             "[::1]:9000",
		"[::1]:1234":        "[::1]:1234",
		"::1":               "[::1]:9000",
		"2001:db8::1":       "[2001:db8::1]:9000",
		"[2001:db8::1]":     "[2001:db8::1]:9000",
		"[fe80::1%eth0]:99": "[fe80::1%eth0]:99",
	} {
		require.Equal(t, want, tcpAddr(in), in)
	}
}

// --- F2: the reader never waits for a write

// gateConn blocks writes while armed; reads work.
type gateConn struct {
	net.Conn
	armed atomic.Bool
	gate  chan struct{}
	once  sync.Once
}

func newGateConn(c net.Conn) *gateConn { return &gateConn{Conn: c, gate: make(chan struct{})} }

func (g *gateConn) Write(b []byte) (int, error) {
	if g.armed.Load() {
		<-g.gate
	}
	return g.Conn.Write(b)
}

func (g *gateConn) open() { g.once.Do(func() { close(g.gate) }) }

func (g *gateConn) Close() error { g.open(); return g.Conn.Close() }

func gatedClient(t *testing.T, f *fakeCamera, cfg Config) (*Client, *gateConn) {
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	g := newGateConn(conn)
	c := NewClient(g, cfg)
	t.Cleanup(func() { c.Close() })
	return c, g
}

func TestReaderNotBlockedByWrites(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.repeat = 200
	c, g := gatedClient(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)
	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	g.armed.Store(true) // the socket stops accepting our writes
	for i := uint16(0); i < 3; i++ {
		f.keepAlive(900 + i)
	}
	// a command is waiting in the queue behind the stuck write too
	require.NoError(t, c.send(&Message{MsgID: MsgPing, MsgNum: 4000, Class: ClassModern24}))

	// inbound media keeps flowing
	seen := 0
	timeout := time.After(5 * time.Second)
	for seen < 100 {
		select {
		case <-st.C:
			seen++
		case <-timeout:
			t.Fatalf("reader stalled behind a blocked write after %d frames", seen)
		}
	}
	require.NoError(t, c.Err())

	// once writes work again the keepalive replies go out first
	g.open()
	for i := uint16(0); i < 3; i++ {
		select {
		case m := <-f.replies:
			require.Equal(t, uint16(900+i), m.MsgNum)
		case <-time.After(2 * time.Second):
			t.Fatal("keepalive reply never written")
		}
	}
}

func TestWriteTimeoutClosesClient(t *testing.T) {
	f := newFakeCamera(t)
	cfg := fastConfig()
	cfg.WriteTimeout = 150 * time.Millisecond
	c, g := gatedClient(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	g.armed.Store(true)
	require.NoError(t, c.send(&Message{MsgID: MsgPing, MsgNum: 1, Class: ClassModern24}))
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("a write stuck for good did not close the client")
	}
	require.ErrorIs(t, c.Err(), ErrWriteTimeout)
}

func TestWriteQueueFullClosesClient(t *testing.T) {
	f := newFakeCamera(t)
	cfg := fastConfig()
	cfg.WriteTimeout = time.Minute
	cfg.WriteQueue = 4
	c, g := gatedClient(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	g.armed.Store(true)
	var failed error
	for i := 0; i < 100 && failed == nil; i++ {
		failed = c.send(&Message{MsgID: MsgPing, MsgNum: uint16(i + 1), Class: ClassModern24})
	}
	require.ErrorIs(t, failed, ErrWriteQueue)
	<-c.Done()

	// the keepalive path fails the same way instead of blocking the reader
	f2 := newFakeCamera(t)
	c2, g2 := gatedClient(t, f2, cfg)
	_, err = c2.Login()
	require.NoError(t, err)
	g2.armed.Store(true)
	for i := uint16(0); i < 40; i++ {
		f2.keepAlive(i + 1)
	}
	select {
	case <-c2.Done():
		require.ErrorIs(t, c2.Err(), ErrWriteQueue)
	case <-time.After(3 * time.Second):
		t.Fatal("keepalive replies piled up without bound")
	}
}

func TestProducerStopDoesNotWaitForWrites(t *testing.T) {
	setVar(t, &stopFlushTimeout, 100*time.Millisecond)
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.talkAbility = adpcmAbility(8000, 400)

	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	g := newGateConn(conn)
	cfg := fastConfig()
	cfg.WriteTimeout = time.Minute
	p, err := NewProducer(g, cfg, StreamMain, "bc://test")
	require.NoError(t, err)

	track := core.NewReceiver(nil, &core.Codec{Name: core.CodecPCMA, ClockRate: 8000})
	require.NoError(t, p.AddTrack(p.GetMedias()[2], track.Codec, track))

	g.armed.Store(true) // every goodbye would block
	start := time.Now()
	require.NoError(t, p.Stop())
	require.Less(t, time.Since(start), 1500*time.Millisecond)
	select {
	case <-p.client.Done():
	default:
		t.Fatal("client not closed by Stop")
	}
}

func TestLogfNotCalledUnderStreamLock(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.repeat = 20

	var st atomic.Pointer[Stream]
	logged := make(chan struct{}, 8)
	cfg := fastConfig()
	cfg.StreamBuffer = 2
	cfg.Log.Warnf = func(string, ...any) {
		// a logger that uses the stream (stats, say) must not deadlock
		if s := st.Load(); s != nil {
			s.mu.Lock()
			s.mu.Unlock() //nolint:staticcheck
		}
		logged <- struct{}{}
	}
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)
	s, err := c.StartStream(StreamMain)
	require.NoError(t, err)
	st.Store(s)

	select {
	case <-logged:
	case <-time.After(3 * time.Second):
		t.Fatal("no drop log, or Logf deadlocked on the stream mutex")
	}
	require.Eventually(t, func() bool { return s.Dropped() > 10 }, 3*time.Second, 5*time.Millisecond, "reader is stuck")
}

// stdio is used by helpers above only in some builds
var _ = io.EOF
var _ = bytes.Equal
