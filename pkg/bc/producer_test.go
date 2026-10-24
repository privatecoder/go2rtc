package bc

import (
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

type recorder struct {
	mu   sync.Mutex
	pkts []*rtp.Packet
}

func (r *recorder) attach(recv *core.Receiver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	orig := recv.Input
	recv.Input = func(p *rtp.Packet) {
		r.mu.Lock()
		r.pkts = append(r.pkts, p)
		r.mu.Unlock()
		orig(p)
	}
}

func (r *recorder) snapshot() []*rtp.Packet {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*rtp.Packet(nil), r.pkts...)
}

func TestProducerSwan(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)

	var d Demuxer
	d.Push(f.stream)
	var wantVideo, wantADPCM int
	for _, m := range drain(&d) {
		switch m.Kind {
		case KindIframe, KindPframe:
			wantVideo++
		case KindADPCM:
			wantADPCM++
		}
	}
	require.NotZero(t, wantVideo)
	require.NotZero(t, wantADPCM)

	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	p, err := NewProducer(conn, fastConfig(), StreamMain, "bc://test")
	require.NoError(t, err)

	// codecs from the probe
	medias := p.GetMedias()
	require.Len(t, medias, 2)
	vc := medias[0].Codecs[0]
	require.Equal(t, core.KindVideo, medias[0].Kind)
	require.Equal(t, core.CodecH264, vc.Name)
	require.Equal(t, uint32(90000), vc.ClockRate)
	require.Equal(t, core.PayloadTypeRAW, vc.PayloadType)
	require.Contains(t, vc.FmtpLine, "sprop-parameter-sets=")
	ac := medias[1].Codecs[0]
	require.Equal(t, core.CodecPCMA, ac.Name)
	require.Equal(t, uint32(8000), ac.ClockRate)

	var video, audio recorder
	video.attach(p.video)
	audio.attach(p.audio)

	errc := make(chan error, 1)
	go func() { errc <- p.Start() }()

	require.Eventually(t, func() bool {
		return len(video.snapshot()) >= wantVideo
	}, 5*time.Second, 5*time.Millisecond, "video packets")

	vp := video.snapshot()
	require.Len(t, vp, wantVideo)

	// the keyframe read while probing is delivered first, and only once
	require.True(t, h264.IsKeyframe(vp[0].Payload))
	for i, pkt := range vp {
		require.Greater(t, len(pkt.Payload), 4)
		if i > 0 {
			require.GreaterOrEqual(t, pkt.Timestamp, vp[i-1].Timestamp)
		}
	}
	require.Greater(t, vp[len(vp)-1].Timestamp, vp[0].Timestamp)

	// adpcm blocks of 240 data bytes -> 480 A-law samples, contiguous clock
	require.Eventually(t, func() bool {
		return len(audio.snapshot()) >= wantADPCM
	}, 5*time.Second, 5*time.Millisecond, "audio packets")
	ap := audio.snapshot()
	require.NotEmpty(t, ap)
	for i, pkt := range ap {
		require.Len(t, pkt.Payload, 480)
		require.True(t, pkt.Marker)
		if i > 0 {
			require.Equal(t, ap[i-1].Timestamp+480, pkt.Timestamp)
			require.Equal(t, ap[i-1].SequenceNumber+1, pkt.SequenceNumber)
		}
	}

	// Stop ends Start and closes the client
	require.NoError(t, p.Stop())
	select {
	case err := <-errc:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// The probe timeout is 15 s, so this only checks that a stream that ends
// without a usable keyframe fails the probe instead of hanging.
func TestProducerNoKeyframe(t *testing.T) {
	// only P-frames
	f := newFakeCamera(t)
	var d Demuxer
	d.Push(swan(t))
	for _, m := range drain(&d) {
		if m.Kind == KindPframe {
			b, err := m.Marshal()
			require.NoError(t, err)
			f.stream = append(f.stream, b...)
		}
	}
	require.NotEmpty(t, f.stream)

	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	go func() {
		<-f.started
		time.Sleep(200 * time.Millisecond)
		f.mu.Lock()
		_ = f.conn.Close()
		f.mu.Unlock()
	}()
	_, err = NewProducer(conn, fastConfig(), StreamMain, "bc://test")
	require.Error(t, err)
}

func TestVideoClock(t *testing.T) {
	var c videoClock
	require.Equal(t, uint64(0), c.next(1_000_000))
	require.Equal(t, uint64(40_000), c.next(1_040_000))

	// the u32 counter wraps
	c = videoClock{}
	c.next(0xFFFF_0000)
	require.Equal(t, uint64(0x1_0000+100), c.next(100))

	// a jump over 10 s is a reset: hold, then continue from the new reference
	c = videoClock{}
	c.next(5_000_000)
	c.next(5_100_000)
	require.Equal(t, uint64(100_000), c.next(900_000_000))
	require.Equal(t, uint64(140_000), c.next(900_040_000))

	// going backwards is also a reset, not a 71 minute leap
	require.Equal(t, uint64(140_000), c.next(800_000_000))

	// exactly 10 s still counts
	c = videoClock{}
	c.next(0)
	require.Equal(t, uint64(10_000_000), c.next(10_000_000))
}

func TestSplitAVCC(t *testing.T) {
	nals, ok := splitAVCC([]byte{0, 0, 0, 2, 0x67, 1, 0, 0, 0, 3, 0x68, 2, 3}, false)
	require.True(t, ok)
	require.Len(t, nals, 2)
	require.Equal(t, byte(7), nals[0].typ)
	require.Equal(t, byte(8), nals[1].typ)

	for name, b := range map[string][]byte{
		"empty":      {},
		"truncated":  {0, 0, 0, 9, 0x67, 1},
		"empty NAL":  {0, 0, 0, 0, 0x67, 1},
		"short":      {1, 2, 3},
		"trailing":   {0, 0, 0, 1, 0x65, 9},
		"huge":       {0xff, 0xff, 0xff, 0xff, 0x65, 1},
		"only size":  {0, 0, 0, 1},
		"size 2 big": {0, 0, 0, 2, 0x65},
	} {
		_, ok = splitAVCC(b, false)
		require.False(t, ok, name)
	}

	// H265 NAL units have a 2 byte header
	_, ok = splitAVCC([]byte{0, 0, 0, 1, 0x40}, true)
	require.False(t, ok)
	nals, ok = splitAVCC([]byte{0, 0, 0, 2, 0x40, 1, 0, 0, 0, 2, 0x26, 1}, true)
	require.True(t, ok)
	require.Equal(t, []byte{32, 19}, []byte{nals[0].typ, nals[1].typ})
}

func TestDialErrors(t *testing.T) {
	_, err := Dial("bc://u:p@95270000ABCDEFGH?discovery=telepathy")
	require.ErrorContains(t, err, "unknown discovery")

	_, err = Dial("bc://u:p@127.0.0.1?stream=huge")
	require.ErrorContains(t, err, "unknown stream")

	// a UID-looking name with a port is a host, not a UID
	_, err = Dial("bc://u:p@ABCDEFGHIJ0:1")
	require.NotContains(t, err.Error(), "discovery")
	_, err = Dial("bc://u:p@ABCDEFGHIJ0:1?discovery=telepathy")
	require.NotContains(t, err.Error(), "unknown discovery")
}
