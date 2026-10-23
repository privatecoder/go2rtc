package bc

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// recorder is a consumer node: a sender whose handler is fn. Moving the
// receiver's children (Receiver.Replace) keeps it attached, like a real consumer.
func attach(t *testing.T, r *core.Receiver, fn core.HandlerFunc) {
	t.Helper()
	s := core.NewSender(nil, r.Codec)
	s.Handler = fn
	s.HandleRTP(r)
	t.Cleanup(s.Close)
}

func newSourceProducer(t *testing.T, f *fakeCamera, source string) *Producer {
	setVar(t, &probeAudioTimeout, 100*time.Millisecond)
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	p, err := NewProducer(conn, fastConfig(), StreamMain, source)
	require.NoError(t, err)
	t.Cleanup(func() { p.Stop() })
	return p
}

// --- 1: audio alone does not keep a stalled video alive

func TestAudioDoesNotHideStalledVideo(t *testing.T) {
	setVar(t, &idleTimeout, 300*time.Millisecond)
	f := newFakeCamera(t)
	f.stream = swan(t)
	p := newFakeProducer(t, f, fastConfig())

	errc := make(chan error, 1)
	go func() { errc <- p.Start() }()

	// audio goes on, video has stopped
	var adpcm *Media
	for _, m := range swanFrames(t) {
		if m.Kind == KindADPCM {
			adpcm = m
		}
	}
	frame := frameBytes(t, adpcm)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
				f.push(frame)
			}
		}
	}()

	select {
	case err := <-errc:
		require.ErrorContains(t, err, "no usable media")
	case <-time.After(3 * time.Second):
		t.Fatal("audio kept a dead video alive")
	}
}

// --- R1: every parameter set is checked

func TestDuplicateShortParameterSet(t *testing.T) {
	fastProbe(t)
	short := []byte{0x67, 0x01}
	good := swanFrames(t)[0]
	sets := nalsOf(t, good, false)

	// the probe: a good SPS and PPS followed by a short SPS (the helper reads all of them)
	m := &Media{Kind: KindIframe, Codec: "H264", Data: annexB(sets[0], sets[1], short, sets[2])}
	f := newFakeCamera(t)
	f.stream = frameBytes(t, m)
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	_, err = NewProducer(conn, fastConfig(), StreamMain, "bc://dup")
	require.ErrorContains(t, err, "no keyframe")

	// the reviewer's case, three byte start codes
	m = &Media{Kind: KindIframe, Codec: "H264", Data: []byte{
		0, 0, 1, 0x67, 0x42, 0x80, 0x31, 0, 0, 1, 0x68, 0x01, 0, 0, 1, 0x65, 0x01, 0, 0, 1, 0x67, 0x01}}
	f2 := newFakeCamera(t)
	f2.stream = frameBytes(t, m)
	conn, err = DialTCP(f2.addr())
	require.NoError(t, err)
	_, err = NewProducer(conn, fastConfig(), StreamMain, "bc://dup")
	require.Error(t, err)

	// H265: a short VPS after the good ones
	hm := h265Key(0)
	hm.Data = annexB(h265VPS, h265SPS, h265PPS, []byte{0x40, 0x01}, h265IDR)
	f3 := newFakeCamera(t)
	f3.stream = frameBytes(t, hm)
	conn, err = DialTCP(f3.addr())
	require.NoError(t, err)
	_, err = NewProducer(conn, fastConfig(), StreamMain, "bc://dup")
	require.ErrorContains(t, err, "no keyframe")

	// at run time: a later keyframe with a short duplicate is damaged, not forwarded
	f4 := newFakeCamera(t)
	f4.stream = swan(t)
	p := newFakeProducer(t, f4, fastConfig())
	var out packets
	p.video.Input = out.add
	bad := &Media{Kind: KindIframe, Codec: "H264", Data: annexB(sets[0], sets[1], short, sets[2])}
	useful, err := p.handle(bad)
	require.NoError(t, err)
	require.False(t, useful)
	require.Empty(t, out.all())
	useful, err = p.handle(good)
	require.NoError(t, err)
	require.True(t, useful)
}

// --- R3: keyframes carry the current parameter sets

func changedSPS(sets [][]byte, i int) [][]byte {
	out := append([][]byte(nil), sets...)
	c := append([]byte(nil), sets[i]...)
	c[len(c)-1] ^= 0x55
	out[i] = c
	return out
}

func firstNAL(avcc []byte) []byte {
	n := int(avcc[0])<<24 | int(avcc[1])<<16 | int(avcc[2])<<8 | int(avcc[3])
	return avcc[4 : 4+n]
}

func TestReplacementKeyframesAreSelfContained(t *testing.T) {
	t.Run("h264", func(t *testing.T) {
		good := swanFrames(t)[0]
		sets := nalsOf(t, good, false) // SPS, PPS, slice...
		newSets := changedSPS(sets, 0)
		keyB := &Media{Kind: KindIframe, Codec: "H264", Data: annexB(newSets...)}

		f1, f2 := newFakeCamera(t), newFakeCamera(t)
		f1.stream, f2.stream = swan(t), frameBytes(t, keyB)
		p1 := newSourceProducer(t, f1, "bc://r3-h264")
		p2 := newSourceProducer(t, f2, "bc://r3-h264")

		// a consumer of the old producer with go2rtc's repair code, then the replacement
		var out packets
		attach(t, p1.video, h264.RepairAVCC(p1.video.Codec, out.add))
		p1.video.Replace(p2.video)

		// the replacement's keyframe, then one that arrives without parameter sets
		_, err := p2.handle(keyB)
		require.NoError(t, err)
		bare := &Media{Kind: KindIframe, Codec: "H264", Data: annexB(newSets[2:]...), Microseconds: 40000, PTS: 40000}
		useful, err := p2.handle(bare)
		require.NoError(t, err)
		require.True(t, useful)

		require.Eventually(t, func() bool { return len(out.all()) == 2 }, 2*time.Second, 5*time.Millisecond)
		for _, pk := range out.all() {
			nals, ok := splitAVCC(pk.Payload, false)
			require.True(t, ok)
			require.Equal(t, byte(7), nals[0].typ)
			require.Equal(t, newSets[0], nals[0].data, "the old SPS came back")
			require.Equal(t, newSets[1], nals[1].data)
		}
	})

	t.Run("h265", func(t *testing.T) {
		keyA := h265Key(0)
		newSets := changedSPS([][]byte{h265VPS, h265SPS, h265PPS}, 1)
		keyB := &Media{Kind: KindIframe, Codec: "H265", Data: annexB(append(newSets, h265IDR)...)}

		f1, f2 := newFakeCamera(t), newFakeCamera(t)
		f1.stream, f2.stream = frameBytes(t, keyA), frameBytes(t, keyB)
		p1 := newSourceProducer(t, f1, "bc://r3-h265")
		p2 := newSourceProducer(t, f2, "bc://r3-h265")

		var out packets
		attach(t, p1.video, h265.RepairAVCC(p1.video.Codec, out.add))
		p1.video.Replace(p2.video)

		_, err := p2.handle(keyB)
		require.NoError(t, err)
		bare := &Media{Kind: KindIframe, Codec: "H265", Data: annexB(h265IDR), PTS: 40000}
		_, err = p2.handle(bare)
		require.NoError(t, err)

		require.Eventually(t, func() bool { return len(out.all()) == 2 }, 2*time.Second, 5*time.Millisecond)
		for _, pk := range out.all() {
			nals, ok := splitAVCC(pk.Payload, true)
			require.True(t, ok)
			require.Equal(t, []byte{32, 33, 34}, []byte{nals[0].typ, nals[1].typ, nals[2].typ})
			require.Equal(t, newSets[1], nals[1].data)
		}
	})

	t.Run("complete keyframes are left alone", func(t *testing.T) {
		f := newFakeCamera(t)
		f.stream = swan(t)
		p := newSourceProducer(t, f, "bc://r3-plain")
		var out packets
		p.video.Input = out.add
		key := swanFrames(t)[0]
		_, err := p.handle(key)
		require.NoError(t, err)
		require.Equal(t, annexb.EncodeToAVCC(key.Data), out.all()[0].Payload)
	})
}

// --- R2: AAC sequence numbers and the per-source timeline

func aacFrames(t *testing.T) (video, aacFrame *Media) {
	video = swanFrames(t)[0]
	aacFrame = &Media{Kind: KindAAC, Data: adts([]byte{1, 2, 3, 4}), AudioRate: 16000}
	return
}

func TestAACSequenceContinuesAcrossReconnect(t *testing.T) {
	video, au := aacFrames(t)
	stream := frameBytes(t, video, &Media{Kind: KindAAC, Data: au.Data})
	f1, f2 := newFakeCamera(t), newFakeCamera(t)
	f1.stream, f2.stream = stream, stream
	p1 := newSourceProducer(t, f1, "bc://r2")
	p2 := newSourceProducer(t, f2, "bc://r2")

	var out packets
	attach(t, p1.audio, out.add)

	// near the end of the 16 bit range, so the wrap is part of it
	p1.src.seq.Store(65533)
	pos := uint64(0)
	send := func(p *Producer, n int) {
		for i := 0; i < n; i++ {
			au.AudioPos = pos
			pos += 1024
			_, err := p.handle(au)
			require.NoError(t, err)
		}
	}
	send(p1, 2)
	require.Eventually(t, func() bool { return len(out.all()) == 2 }, 2*time.Second, 5*time.Millisecond)
	p1.audio.Replace(p2.audio) // go2rtc moves the consumers to the replacement
	send(p2, 4)

	require.Eventually(t, func() bool { return len(out.all()) == 6 }, 2*time.Second, 5*time.Millisecond)
	pk := out.all()
	for i, x := range pk {
		require.Equal(t, uint16(65534+i), x.SequenceNumber, "packet %d", i)
		require.Zero(t, x.SSRC)
		if i > 0 {
			require.Greater(t, x.Timestamp, pk[i-1].Timestamp)
		}
	}
}

func TestTimelineIsPerSource(t *testing.T) {
	f1, f2 := newFakeCamera(t), newFakeCamera(t)
	f1.stream, f2.stream = swan(t), swan(t)
	pa := newSourceProducer(t, f1, "bc://source-a")
	pb := newSourceProducer(t, f2, "bc://source-b")
	require.NotSame(t, pa.src, pb.src)
	require.Same(t, pa.src, sourceOf("bc://source-a"))

	// one camera's timeline is far ahead
	pa.src.endUS.Store(pa.src.endUS.Load() + 3_600_000_000)

	key := swanFrames(t)[0]
	var a, b packets
	pa.video.Input, pb.video.Input = a.add, b.add
	pa2 := newSourceProducer(t, newFakeCameraWith(t, swan(t)), "bc://source-a") // its replacement
	pa2.video.Input = a.add
	_, err := pa2.handle(key)
	require.NoError(t, err)
	_, err = pb.handle(key)
	require.NoError(t, err)

	require.Greater(t, uint64(a.all()[0].Timestamp), uint64(3_600_000_000)*9/100, "the source continues after itself")
	require.Less(t, uint64(b.all()[0].Timestamp), uint64(600_000_000)*9/100, "and not after a camera that is not it")
}

func newFakeCameraWith(t *testing.T, stream []byte) *fakeCamera {
	f := newFakeCamera(t)
	f.stream = stream
	return f
}

// --- 5: the audio clock follows the video clock

func TestAudioClockReanchorsOnDrift(t *testing.T) {
	var a audioClock
	pos, re := a.next(16000, 1024, 1_000_000, true)
	require.Equal(t, uint64(16000), pos)
	require.False(t, re)

	// a little drift is normal
	pos, re = a.next(16000, 1024, 1_300_000, true) // audio at 1.064 s, video 1.3 s
	require.False(t, re)
	require.Equal(t, uint64(17024), pos)

	// frames were lost: video is 3 s on, the audio clock only moved 128 ms
	pos, re = a.next(16000, 1024, 3_000_000, true)
	require.True(t, re)
	require.Equal(t, uint64(48000), pos, "back on the video clock")
	pos, _ = a.next(16000, 1024, 3_040_000, true)
	require.Equal(t, uint64(48000+1024), pos)

	// audio running ahead is corrected as well, and without video nothing is
	pos, re = a.next(16000, 1024, 1_000_000, true)
	require.True(t, re)
	require.Equal(t, uint64(16000), pos)
	pos, re = a.next(16000, 1024, 99_000_000, false)
	require.False(t, re)
	require.Equal(t, uint64(16000+1024), pos)
}

// --- R5: no talk object or goroutine survives a dead transport

func TestTalkEndsWithTheTransport(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	c, cfg := startTalkClient(t, f)
	base := runtime.NumGoroutine()

	talk, err := c.StartTalk(cfg)
	require.NoError(t, err)
	c.fail(errors.New("test: transport died"))

	select {
	case <-talk.Ended():
	case <-time.After(2 * time.Second):
		t.Fatal("the talk is still open after the transport died")
	}
	c.mu.Lock()
	require.Nil(t, c.talk, "the client still holds the talk")
	c.mu.Unlock()
	c.Close()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= base+2 }, 2*time.Second, 20*time.Millisecond)
}

func TestProducerTalkEndsWhenTheTransportDies(t *testing.T) {
	fastIdle(t)
	f0 := newFakeCamera(t)
	f0.talkAbility = adpcmAbility(8000, 400)
	{ // warm up so the base count is stable
		p := newTalkProducer(t, f0)
		_ = p
	}
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	for i := 0; i < 6; i++ {
		f := newFakeCamera(t)
		f.talkAbility = adpcmAbility(8000, 400)
		p := newTalkProducer(t, f)
		back := p.GetMedias()[2]
		r := mic()
		require.NoError(t, p.AddTrack(back, pcmaCodec, r))
		feed(r, tone(401, true))
		require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)

		p.client.mu.Lock()
		talk := p.client.talk
		p.client.mu.Unlock()
		require.NotNil(t, talk)

		_ = p.client.conn.Close() // the connection dies under the producer
		select {
		case <-talk.Ended():
		case <-time.After(2 * time.Second):
			t.Fatal("talk still open")
		}
		require.Eventually(t, func() bool { return p.hub.count() == 0 }, 2*time.Second, 5*time.Millisecond)
		require.NoError(t, p.Stop())
	}
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= base+8 }, 5*time.Second, 50*time.Millisecond,
		"goroutines pile up: %d, base %d", runtime.NumGoroutine(), base)
}

// --- R7: a refusal is followed by the ordered reset

func TestRefusedBlockWaitsForTheResetAnswer(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.ackCode = 400
	f.resetDelay = time.Second
	f.talkExclusive = true
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	// one packet with four blocks (the refusal comes while the rest is paced),
	// and another packet right behind it that would start a successor
	pcmaPush(r, tone(4*401, true))
	pcmaPush(r, tone(401, true))

	require.Eventually(t, func() bool { return len(f.sessionEvents()) >= 3 }, 5*time.Second, 5*time.Millisecond)
	ev := f.sessionEvents()
	require.Equal(t, []string{"config:200", "reset", "config:200"}, ev[:3])
	gap := f.eventTime("config:200", 1).Sub(f.eventTime("reset", 0))
	require.GreaterOrEqual(t, gap, 900*time.Millisecond, "the successor did not wait for the reset to be answered")
}

// --- R8: the writer's stalls do not turn into bursts

func TestStalledWriterDoesNotMakeABurst(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stall time.Duration
		stale bool
	}{
		{"short stall", 200 * time.Millisecond, false},
		{"long stall", 800 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeCamera(t)
			f.stream = swan(t)
			f.talkAbility = adpcmAbility(8000, 400) // 401 samples, 50 ms
			conn, err := DialTCP(f.addr())
			require.NoError(t, err)
			g := newGateConn(conn)
			cfg := fastConfig()
			cfg.WriteTimeout = time.Minute
			p, err := NewProducer(g, cfg, StreamMain, "bc://stall")
			require.NoError(t, err)
			t.Cleanup(func() { p.Stop() })
			back := p.GetMedias()[2]

			r := mic()
			require.NoError(t, p.AddTrack(back, pcmaCodec, r))
			feed(r, tone(8*401, true)) // eight blocks of audio at once
			require.Eventually(t, func() bool { return len(f.blocks()) >= 1 }, 3*time.Second, 5*time.Millisecond)

			g.armed.Store(true) // the writer stalls
			time.Sleep(tc.stall)
			resume := time.Now()
			g.armed.Store(false)
			g.open()

			if tc.stale {
				// the audio of the stall is old and dropped, not played in a burst
				time.Sleep(300 * time.Millisecond)
				require.Less(t, len(f.blocks()), 8)
				return
			}

			require.Eventually(t, func() bool { return len(f.blocks()) >= 5 }, 3*time.Second, 5*time.Millisecond)
			blocks := f.blocks()
			for i := 2; i < 5; i++ {
				gap := blocks[i].at.Sub(blocks[i-1].at)
				require.GreaterOrEqual(t, gap, 35*time.Millisecond, "blocks %d and %d arrived as a burst, %s after the resume", i-1, i, blocks[i].at.Sub(resume))
			}
		})
	}
}

func TestFinalDrainStartsAfterTheLastWrite(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.talkAbility = adpcmAbility(8000, 400)
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	g := newGateConn(conn)
	cfg := fastConfig()
	cfg.WriteTimeout = time.Minute
	p, err := NewProducer(g, cfg, StreamMain, "bc://drain")
	require.NoError(t, err)
	t.Cleanup(func() { p.Stop() })
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)

	// the tail leaves while the writer is stalled, then the sender is closed
	feed(r, tone(200, true))
	g.armed.Store(true)
	time.Sleep(50 * time.Millisecond)
	p.hub.mu.Lock()
	sender := p.hub.talkers[0].sender
	p.hub.mu.Unlock()
	sender.Close()
	time.Sleep(300 * time.Millisecond)
	g.armed.Store(false)
	g.open()

	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, 3*time.Second, 5*time.Millisecond)
	gap := f.eventTime("reset", 0).Sub(f.blocks()[1].at)
	require.GreaterOrEqual(t, gap, 401*time.Second/8000+talkDrain-10*time.Millisecond, "the reset cut off the tail that was just written")
}

var _ = atomic.Int32{}
var _ = rtp.Packet{}
