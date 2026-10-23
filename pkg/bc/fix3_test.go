package bc

import (
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

var pcmaCodec = &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}

func mic() *core.Receiver { return core.NewReceiver(nil, pcmaCodec) }

// --- F1, F2: ownership follows the audio, lifetimes are bounded

func TestReconnectReplayAttachesTheLiveMicrophone(t *testing.T) {
	fastIdle(t)
	setVar(t, &talkRetire, time.Hour)

	f1 := newFakeCamera(t)
	f1.talkAbility = adpcmAbility(8000, 400)
	f1.talkExclusive = true
	p1 := newTalkProducer(t, f1)
	back := p1.GetMedias()[2]

	// the first microphone talks and goes away (its track is only detached)
	m1 := mic()
	require.NoError(t, p1.AddTrack(back, pcmaCodec, m1))
	feed(m1, tone(401, true))
	require.Eventually(t, func() bool { return len(f1.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)
	m1.Close()
	require.Eventually(t, func() bool { return len(f1.sessionEvents()) == 2 }, 3*time.Second, 5*time.Millisecond)

	// the second one takes over
	m2 := mic()
	require.NoError(t, p1.AddTrack(back, pcmaCodec, m2))
	feed(m2, tone(401, true))
	require.Eventually(t, func() bool { return len(f1.blocks()) == 2 }, 3*time.Second, 5*time.Millisecond)

	// the camera connection is replaced: internal/streams attaches every
	// microphone it ever saw to the new producer, oldest first, and ignores errors
	f2 := newFakeCamera(t)
	f2.talkAbility = adpcmAbility(8000, 400)
	f2.talkExclusive = true
	p2 := newTalkProducer(t, f2)
	back2 := p2.GetMedias()[2]
	for _, r := range []*core.Receiver{m1, m2} {
		require.NoError(t, p2.AddTrack(back2, pcmaCodec, r), "no attachment may fail")
	}
	require.NoError(t, p1.Stop()) // after the move, like go2rtc

	// the live microphone reaches the replacement camera
	feed(m2, tone(802, true))
	require.Eventually(t, func() bool { return len(f2.blocks()) >= 2 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, "config:200", f2.sessionEvents()[0])
	require.Empty(t, f2.violationList())
	require.Empty(t, f1.violationList())
}

func TestTalkersDoNotAccumulate(t *testing.T) {
	setVar(t, &talkIdle, 80*time.Millisecond)
	setVar(t, &talkRetire, 150*time.Millisecond)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.talkExclusive = true
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	var base int
	for i := 0; i < 10; i++ {
		r := mic()
		require.NoError(t, p.AddTrack(back, pcmaCodec, r))
		feed(r, tone(401, true))
		require.Eventually(t, func() bool { return len(f.blocks()) == i+1 }, 3*time.Second, 5*time.Millisecond)
		r.Close()
		time.Sleep(260 * time.Millisecond) // idle, then retired by the next AddTrack

		if i == 1 {
			base = runtime.NumGoroutine()
		}
		require.LessOrEqual(t, p.hub.count(), 3, "talkers pile up")
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), base+6, "goroutines pile up")
	require.Empty(t, f.violationList())

	p.hub.mu.Lock()
	n := len(p.hub.talkers)
	p.hub.mu.Unlock()
	require.LessOrEqual(t, n, 3)
	require.Empty(t, p.Senders, "talk senders are not left in the connection")
}

func TestMostRecentAudioWins(t *testing.T) {
	fastIdle(t)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.talkExclusive = true
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	a, b := mic(), mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, a))
	require.NoError(t, p.AddTrack(back, pcmaCodec, b))

	feed(a, tone(802, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 2 }, 3*time.Second, 5*time.Millisecond)

	// a is talking, b is not let in
	feed(b, tone(401, true))
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, []string{"config:200"}, f.sessionEvents())
	require.Len(t, f.blocks(), 2)

	// a falls silent; its session ends by itself and b can start
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, 3*time.Second, 5*time.Millisecond)
	feed(b, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 3 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset", "config:200"}, f.sessionEvents())
	require.Empty(t, f.violationList())

	// b is silent for the idle time but a speaks again: a takes the session back
	// (the old session is ended first)
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 4 }, 3*time.Second, 5*time.Millisecond)
	feed(a, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 4 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset", "config:200", "reset", "config:200"}, f.sessionEvents())
	require.Empty(t, f.violationList())
}

func TestTakeoverFromASilentOwner(t *testing.T) {
	// the owner's idle timer has not fired yet but it has been silent as long
	// as the idle time: the newcomer asks it to end the session and takes over
	setVar(t, &talkIdle, 150*time.Millisecond)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.talkExclusive = true
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	a, b := mic(), mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, a))
	require.NoError(t, p.AddTrack(back, pcmaCodec, b))
	feed(a, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)

	var owner, newcomer *talker
	p.hub.mu.Lock()
	owner, newcomer = p.hub.talkers[0], p.hub.talkers[1]
	p.hub.mu.Unlock()
	require.Equal(t, owner, p.hub.owner)

	owner.lastAudio.Store(time.Now().Add(-time.Second).UnixNano()) // silent for 1 s as far as the hub knows
	feed(b, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 2 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset", "config:200"}, f.sessionEvents())
	require.Empty(t, f.violationList())
	p.hub.mu.Lock()
	require.Equal(t, newcomer, p.hub.owner)
	p.hub.mu.Unlock()
}

// --- F3: transitions are ordered

func TestTalkSessionOrdering(t *testing.T) {
	for i := 0; i < 12; i++ {
		f := newFakeCamera(t)
		f.talkAbility = adpcmAbility(8000, 400)
		f.talkExclusive = true
		c, cfg := startTalkClient(t, f)

		t1, err := c.StartTalk(cfg)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // blocks while the session is being ended
			defer wg.Done()
			var enc ADPCMEncoder
			block, _ := enc.EncodeBlock(nil, t1.DataBytes)
			for j := 0; j < 200; j++ {
				if err := t1.Send(block); err != nil {
					require.ErrorIs(t, err, ErrTalkStopped)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			require.NoError(t, t1.Stop())
		}()

		// the successor starts while the reset is in flight: it waits for it
		require.Eventually(t, t1.Stopped, time.Second, 100*time.Microsecond)
		t2, err := c.StartTalk(cfg)
		require.NoError(t, err)
		wg.Wait()

		require.Equal(t, []string{"config:200", "reset", "config:200"}, f.sessionEvents())
		require.Empty(t, f.violationList(), "a block outside a session")

		// no block of the first session after its reset
		resetAt := f.eventTime("reset", 0)
		for _, b := range f.blocks() {
			if b.num == t1.num {
				require.False(t, b.at.After(resetAt), "block after its session's reset")
			}
		}

		// and the successor works
		var enc ADPCMEncoder
		block, _ := enc.EncodeBlock(nil, t2.DataBytes)
		require.NoError(t, t2.Send(block))
		require.NoError(t, t2.Stop())
		require.Equal(t, []string{"config:200", "reset", "config:200", "reset"}, f.sessionEvents())
	}
}

func TestStartTalkWaitsForTheReset(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.talkExclusive = true
	f.resetDelay = 300 * time.Millisecond // the camera is slow to answer the reset
	c, cfg := startTalkClient(t, f)

	t1, err := c.StartTalk(cfg)
	require.NoError(t, err)
	go t1.Stop()
	require.Eventually(t, t1.Stopped, time.Second, time.Millisecond)

	// the slot is still the old session's, a new one must neither fail nor jump the reset
	start := time.Now()
	t2, err := c.StartTalk(cfg)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset", "config:200"}, f.sessionEvents())

	// an active session is not waited for
	_, err = c.StartTalk(cfg)
	require.ErrorIs(t, err, ErrTalkBusy)
	require.NoError(t, t2.Stop())
}

// --- F4: nothing waits for a sleep or the camera when stopping

func TestStopDuringPacing(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 6400) // a block is 6401 samples, 0.8 s
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(3*6401, true)) // three blocks at once, the second waits for its slot
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // the worker sleeps

	start := time.Now()
	require.NoError(t, p.Stop())
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// the camera was told, without waiting for the block to play
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, time.Second, 5*time.Millisecond)
	require.Less(t, f.eventTime("reset", 0).Sub(f.blocks()[0].at), 600*time.Millisecond)
	require.Len(t, f.blocks(), 1, "no more blocks after the stop")
}

func TestStopWhileTheConfigIsAnswered(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.configDelay = 3 * time.Second
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(401, true))
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 1 }, time.Second, 5*time.Millisecond)

	start := time.Now()
	require.NoError(t, p.Stop())
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// the camera may have accepted the config: it gets the reset anyway
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset"}, f.sessionEvents())
}

func TestStopWhileTheResetIsAnswered(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.resetDelay = 3 * time.Second
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, time.Second, 5*time.Millisecond)

	p.hub.mu.Lock()
	sender := p.hub.talkers[0].sender
	p.hub.mu.Unlock()
	sender.Close() // graceful end: the worker now waits for the camera's answer to the reset
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, time.Second, 5*time.Millisecond)

	start := time.Now()
	require.NoError(t, p.Stop())
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestStopWithManyBlocksInOnePacket(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400) // 401 samples, 50 ms
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	big := tone(60*401, true) // one packet that holds 60 blocks, 3 s of audio
	pcmaPush(r, big)
	require.Eventually(t, func() bool { return len(f.blocks()) >= 2 }, 3*time.Second, 5*time.Millisecond)

	start := time.Now()
	require.NoError(t, p.Stop())
	require.Less(t, time.Since(start), 500*time.Millisecond)
	n := len(f.blocks())
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, n, len(f.blocks()), "the rest of the packet is dropped, not sent")
	require.Less(t, n, 10)
}

// --- F5: the end of the audio is not lost

func TestShortUtteranceIsSent(t *testing.T) {
	fastIdle(t)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400) // 401 samples per block
	f.talkExclusive = true
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	// less than one block
	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(400, true))
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, 3*time.Second, 5*time.Millisecond)
	require.Len(t, f.blocks(), 1, "the utterance leaves as one padded block")
	block, _ := parseTalk(t, f.blocks()[0].data)
	require.Len(t, block, 204)
	got := decodeIMALowFirst(block)
	require.Len(t, got, 401)
	require.InDelta(t, float64(got[399]), float64(got[400]), 2000, "padded with the last sample")
	gap := f.eventTime("reset", 0).Sub(f.blocks()[0].at)
	require.GreaterOrEqual(t, gap, 401*time.Second/8000+talkDrain-10*time.Millisecond, "reset after playback and margin")

	// a block and a bit
	feed(r, tone(401+150, true))
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 4 }, 3*time.Second, 5*time.Millisecond)
	require.Len(t, f.blocks(), 3)
	require.Equal(t, []string{"config:200", "reset", "config:200", "reset"}, f.sessionEvents())
	last := f.blocks()[2].at
	require.GreaterOrEqual(t, f.eventTime("reset", 1).Sub(last), 401*time.Second/8000+talkDrain-10*time.Millisecond)
	require.Empty(t, f.violationList())
}

func TestForcedStopDoesNotWaitForPlayback(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 6400) // the block plays for 0.8 s
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(6401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)

	require.NoError(t, p.Stop())
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, time.Second, 5*time.Millisecond)
	require.Less(t, f.eventTime("reset", 0).Sub(f.blocks()[0].at), 400*time.Millisecond, "an abort, not a drain")
}

// --- F6: the resampler

func TestResamplerPacketsDoNotMatter(t *testing.T) {
	const n = 16000 // two seconds at 8 kHz
	src := make([]int16, n)
	for i := range src {
		src[i] = int16(1000 + i%20000/2) // positive, rising: a zero is a bug
	}

	for _, dst := range []uint32{8000, 11025, 16000, 22050, 44100, 48000} {
		whole := newResampler(8000, dst).process(src)
		if dst != 8000 {
			want := ((n-1)*int(dst) + 7999) / 8000 // outputs while position < last sample
			require.Len(t, whole, want, "to %d", dst)
		}
		for _, v := range whole {
			require.Greater(t, v, int16(0), "an inserted zero, to %d", dst)
		}

		for _, size := range []int{1, 7, 160, 333, 1000, n} {
			rs := newResampler(8000, dst)
			var got []int16
			for off := 0; off < n; off += size {
				got = append(got, rs.process(src[off:min(off+size, n)])...)
			}
			require.Equal(t, whole, got, "to %d in packets of %d", dst, size)
		}
	}
}

func TestResamplerValues(t *testing.T) {
	// 8 kHz to 16 kHz: every sample and the mean with its successor
	rs := newResampler(8000, 16000)
	out := rs.process([]int16{0, 100, 200, 300})
	require.Equal(t, []int16{0, 50, 100, 150, 200, 250}, out)
	out = rs.process([]int16{400})
	require.Equal(t, []int16{300, 350}, out, "the packet boundary is invisible")

	// down: 48 kHz to 8 kHz takes every 6th sample
	ramp := make([]int16, 60)
	for i := range ramp {
		ramp[i] = int16(i * 10)
	}
	require.Equal(t, []int16{0, 60, 120, 180, 240, 300, 360, 420, 480, 540}, newResampler(48000, 8000).process(ramp)[:10])

	// same rate: a copy
	in := []int16{1, 2, 3}
	same := newResampler(8000, 8000).process(in)
	require.Equal(t, in, same)
	same[0] = 9
	require.Equal(t, int16(1), in[0])

	require.Nil(t, newResampler(8000, 16000).process(nil))
}

func TestProducerTalkAtAFractionalRate(t *testing.T) {
	// the camera takes 11.025 kHz: nothing is padded or lost on the way
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(11025, 400)
	fastIdle(t)
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(8000, true)) // one second

	want := ((7999*11025 + 7999) / 8000) // samples out
	blocks := (want + 400) / 401         // whole blocks and the padded tail
	require.Eventually(t, func() bool { return len(f.blocks()) == blocks }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, blocks, len(f.blocks()))
}

// --- F7: the negotiated size

func TestSendNeedsTheNegotiatedBlock(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	c, cfg := startTalkClient(t, f)
	talk, err := c.StartTalk(cfg)
	require.NoError(t, err)
	defer talk.Stop()
	require.Equal(t, 200, talk.DataBytes)

	good := make([]byte, 204)
	for name, b := range map[string][]byte{
		"empty":        nil,
		"header only":  make([]byte, 4),
		"one short":    make([]byte, 203),
		"one long":     make([]byte, 205),
		"double":       make([]byte, 408),
		"step index":   {0, 0, 89, 0, 1}, // too short as well, see below
		"bad index":    append([]byte{0, 0, 89, 0}, make([]byte, 200)...),
		"index 255":    append([]byte{0, 0, 255, 0}, make([]byte, 200)...),
		"legacy block": make([]byte, 4+200+1),
	} {
		require.Error(t, talk.Send(b), name)
	}
	time.Sleep(30 * time.Millisecond)
	require.Empty(t, f.blocks(), "nothing reached the wire")

	good[2] = 88
	require.NoError(t, talk.Send(good))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, time.Second, time.Millisecond)
}

// --- F8: statistics

func TestStatsCanBeReadWhileTalking(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				b, err := json.Marshal(p)
				if err != nil {
					t.Error(err)
					return
				}
				var info map[string]any
				if err = json.Unmarshal(b, &info); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	feed(r, tone(401*4, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 4 }, 3*time.Second, 5*time.Millisecond)
	close(stop)
	wg.Wait()

	b, err := json.Marshal(p)
	require.NoError(t, err)
	var info struct {
		FormatName string `json:"format_name"`
		Send       int    `json:"bytes_send"`
		Medias     []any  `json:"medias"`
	}
	require.NoError(t, json.Unmarshal(b, &info))
	require.Equal(t, "bc", info.FormatName)
	require.Equal(t, 4*204, info.Send)
	require.Len(t, info.Medias, 3)
}

// --- open points: the wire form of the config, the policy for refused blocks

func TestTalkConfigWire(t *testing.T) {
	// the body of msg 201 as neolink writes it, the version attribute empty
	cfg, err := NewTalkConfig(adpcmAbility(8000, 400), 0)
	require.NoError(t, err)
	require.Equal(t, "", cfg.Version)

	b, err := (&Body{TalkConfig: cfg}).Marshal()
	require.NoError(t, err)
	require.Equal(t, "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"+
		`<body><TalkConfig version=""><channelId>0</channelId><duplex>FDX</duplex>`+
		`<audioStreamMode>followVideoStream</audioStreamMode><audioConfig><audioType>adpcm</audioType>`+
		`<sampleRate>8000</sampleRate><samplePrecision>16</samplePrecision><lengthPerEncoder>400</lengthPerEncoder>`+
		`<soundTrack>mono</soundTrack></audioConfig></TalkConfig></body>`, string(b))

	// and what the camera receives
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	c, got := startTalkClient(t, f)
	talk, err := c.StartTalk(got)
	require.NoError(t, err)
	defer talk.Stop()
	f.mu.Lock()
	require.Equal(t, "", f.talkConfig[0].Version)
	require.Equal(t, uint16(8000), f.talkConfig[0].AudioConfig.SampleRate)
	f.mu.Unlock()
}

func TestRefusedBlockEndsTheSession(t *testing.T) {
	var warns atomic.Int32
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.ackCode = 400
	f.talkExclusive = true
	cfg := fastConfig()
	cfg.Log = Logger{Warnf: func(string, ...any) { warns.Add(1) }}

	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	f.stream = swan(t)
	p, err := NewProducer(conn, cfg, StreamMain, "bc://test")
	require.NoError(t, err)
	t.Cleanup(func() { p.Stop() })
	back := p.GetMedias()[2]

	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(401*4, true))

	// the first answer refuses: the session is reset (gracefully) and it is logged
	require.Eventually(t, func() bool { return len(f.sessionEvents()) >= 2 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, "reset", f.sessionEvents()[1])
	require.Eventually(t, func() bool { return warns.Load() >= 1 }, time.Second, 5*time.Millisecond)
	require.Empty(t, f.violationList())
}

func TestUnansweredBlocksAreNotAnError(t *testing.T) {
	// pacing alone drives the sending; a camera that never answers is fine
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.ackCode = 0
	p := newTalkProducer(t, f)
	back := p.GetMedias()[2]
	r := mic()
	require.NoError(t, p.AddTrack(back, pcmaCodec, r))
	feed(r, tone(401*3, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 3 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200"}, f.sessionEvents())
}

// pcmaPush writes one RTP packet with the whole payload.
func pcmaPush(track *core.Receiver, payload []byte) {
	track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2}, Payload: payload})
}
