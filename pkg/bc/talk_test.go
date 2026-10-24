package bc

import (
	"math"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func TestNewTalkConfig(t *testing.T) {
	ability := adpcmAbility(16000, 1024)
	ability.AudioConfigList[0].AudioConfig.Priority = new(uint32)

	cfg, err := NewTalkConfig(ability, 2)
	require.NoError(t, err)
	require.Equal(t, uint8(2), cfg.ChannelID)
	require.Equal(t, "FDX", cfg.Duplex)
	require.Equal(t, "followVideoStream", cfg.AudioStreamMode)
	require.Equal(t, ability.AudioConfigList[0].AudioConfig, cfg.AudioConfig, "copied as the camera sent it")

	rate, dataBytes, err := TalkFormat(cfg)
	require.NoError(t, err)
	require.Equal(t, uint32(16000), rate)
	require.Equal(t, 512, dataBytes) // block_size = lengthPerEncoder/2 + 4 with its header

	// no stream mode listed: the default
	ability = adpcmAbility(8000, 400)
	ability.AudioStreamModeList = []AudioStreamModeList{{}}
	cfg, err = NewTalkConfig(ability, 0)
	require.NoError(t, err)
	require.Equal(t, "followVideoStream", cfg.AudioStreamMode)

	for name, mod := range map[string]func(*TalkAbility){
		"no duplex":   func(a *TalkAbility) { a.DuplexList = nil },
		"no modes":    func(a *TalkAbility) { a.AudioStreamModeList = nil },
		"no config":   func(a *TalkAbility) { a.AudioConfigList = nil },
		"not adpcm":   func(a *TalkAbility) { a.AudioConfigList[0].AudioConfig.AudioType = "g711a" },
		"no rate":     func(a *TalkAbility) { a.AudioConfigList[0].AudioConfig.SampleRate = 0 },
		"no length":   func(a *TalkAbility) { a.AudioConfigList[0].AudioConfig.LengthPerEncoder = 1 },
		"zero length": func(a *TalkAbility) { a.AudioConfigList[0].AudioConfig.LengthPerEncoder = 0 },
	} {
		a := adpcmAbility(8000, 400)
		mod(a)
		_, err = NewTalkConfig(a, 0)
		require.Error(t, err, name)
	}
	_, err = NewTalkConfig(nil, 0)
	require.Error(t, err)
}

func TestTalkAbilityRequest(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(16000, 1024)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	a, err := c.GetTalkAbility()
	require.NoError(t, err)
	require.Equal(t, uint16(16000), a.AudioConfigList[0].AudioConfig.SampleRate)

	f.mu.Lock()
	f.talkAbility = nil
	f.mu.Unlock()
	_, err = c.GetTalkAbility()
	require.Error(t, err)
}

func startTalkClient(t *testing.T, f *fakeCamera) (*Client, TalkConfig) {
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)
	cfg, err := NewTalkConfig(f.talkAbility, 0)
	require.NoError(t, err)
	return c, *cfg
}

func TestStartTalkSendStop(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	c, cfg := startTalkClient(t, f)

	talk, err := c.StartTalk(cfg)
	require.NoError(t, err)
	require.Equal(t, uint32(8000), talk.SampleRate)
	require.Equal(t, 200, talk.DataBytes)
	require.Equal(t, []string{"config:200"}, f.events())

	f.mu.Lock()
	require.Equal(t, cfg, f.talkConfig[0])
	f.mu.Unlock()

	// one session at a time
	_, err = c.StartTalk(cfg)
	require.ErrorIs(t, err, ErrTalkBusy)
	require.Equal(t, []string{"config:200"}, f.events(), "the refused one sent nothing")

	var enc ADPCMEncoder
	for i := 0; i < 3; i++ {
		block, err := enc.EncodeBlock(nil, talk.DataBytes)
		require.NoError(t, err)
		require.NoError(t, talk.Send(block))
	}
	require.Eventually(t, func() bool { return len(f.blocks()) == 3 }, 2*time.Second, time.Millisecond)

	// all blocks share one msg_num, which is not the config's
	f.mu.Lock()
	require.Len(t, f.talkNums, 1)
	f.mu.Unlock()
	for _, b := range f.blocks() {
		block, rest := parseTalk(t, b.data)
		require.Len(t, block, 4+200)
		require.Empty(t, rest)
	}

	require.NoError(t, talk.Stop())
	require.NoError(t, talk.Stop()) // idempotent
	require.Equal(t, []string{"config:200", "reset"}, f.sessionEvents())
	require.ErrorIs(t, talk.Send(make([]byte, 204)), ErrTalkStopped)

	// the slot is free again
	talk2, err := c.StartTalk(cfg)
	require.NoError(t, err)
	require.NoError(t, talk2.Stop())
}

func TestStartTalk422ResetsAndRetries(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	f.talkFail = 1
	c, cfg := startTalkClient(t, f)

	talk, err := c.StartTalk(cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"config:422", "reset", "config:200"}, f.events())
	require.NoError(t, talk.Stop())

	// a second 422 is final and releases the session slot
	f.mu.Lock()
	f.talkFail, f.talkEvents, f.eventAt = 2, nil, nil
	f.mu.Unlock()
	_, err = c.StartTalk(cfg)
	require.ErrorContains(t, err, "422")
	require.Equal(t, []string{"config:422", "reset", "config:422"}, f.events())

	f.mu.Lock()
	f.talkFail = 0
	f.mu.Unlock()
	talk, err = c.StartTalk(cfg)
	require.NoError(t, err)
	require.NoError(t, talk.Stop())
}

func TestStartTalkRejectsBadFormat(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	c, cfg := startTalkClient(t, f)

	cfg.AudioConfig.AudioType = "g711a"
	_, err := c.StartTalk(cfg)
	require.Error(t, err)
	require.Empty(t, f.events(), "nothing sent")
}

func TestProducerNoTalk(t *testing.T) {
	// refused ability, or a format that is not adpcm: stream without backchannel
	for name, ability := range map[string]*TalkAbility{
		"refused": nil,
		"g711": func() *TalkAbility {
			a := adpcmAbility(8000, 400)
			a.AudioConfigList[0].AudioConfig.AudioType = "g711a"
			return a
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeCamera(t)
			f.stream = swan(t)
			f.talkAbility = ability
			conn, err := DialTCP(f.addr())
			require.NoError(t, err)
			p, err := NewProducer(conn, fastConfig(), StreamMain, "bc://test")
			require.NoError(t, err)
			defer p.Stop()

			require.Len(t, p.GetMedias(), 2)
			for _, m := range p.GetMedias() {
				require.Equal(t, core.DirectionRecvonly, m.Direction)
			}
			require.Error(t, p.AddTrack(nil, nil, core.NewReceiver(nil, &core.Codec{Name: core.CodecPCMA, ClockRate: 8000})))
		})
	}
}

// tone is n samples of a 600 Hz sine as G.711 at 8 kHz.
func tone(n int, alaw bool) []byte {
	b := make([]byte, n)
	for i := range b {
		s := int16(8000 * math.Sin(2*math.Pi*600*float64(i)/8000))
		if alaw {
			b[i] = pcm.PCMtoPCMA(s)
		} else {
			b[i] = pcm.PCMtoPCMU(s)
		}
	}
	return b
}

// feed writes g711 to the track in 20 ms packets as fast as it can.
func feed(track *core.Receiver, g711 []byte) {
	var seq uint16
	for off := 0; off < len(g711); off += 160 {
		seq++
		track.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: uint32(off)},
			Payload: g711[off:min(off+160, len(g711))],
		})
	}
}

func newTalkProducer(t *testing.T, f *fakeCamera) *Producer {
	f.stream = swan(t)
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	p, err := NewProducer(conn, fastConfig(), StreamMain, "bc://test")
	require.NoError(t, err)
	t.Cleanup(func() { p.Stop() })
	return p
}

func fastIdle(t *testing.T) {
	setVar(t, &talkIdle, 200*time.Millisecond)
}

// lerp2 is the 8 kHz tone at 16 kHz with linear interpolation: every source
// sample, and the mean of it and the next in between.
func lerp2(src []byte, decode func(byte) int16, n int) []int16 {
	out := make([]int16, n)
	for i := range out {
		a := int64(decode(src[i/2]))
		if i%2 == 0 {
			out[i] = int16(a)
		} else {
			b := int64(decode(src[i/2+1]))
			out[i] = int16(a + (b-a)/2)
		}
	}
	return out
}

func TestProducerTalk(t *testing.T) {
	fastIdle(t)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(16000, 512) // 256 data bytes, 513 samples = 32 ms per block
	p := newTalkProducer(t, f)

	medias := p.GetMedias()
	require.Len(t, medias, 3)
	back := medias[2]
	require.Equal(t, core.DirectionSendonly, back.Direction)
	require.Equal(t, core.KindAudio, back.Kind)
	require.Len(t, back.Codecs, 2)
	require.Equal(t, core.CodecPCMA, back.Codecs[0].Name)
	require.Equal(t, uint32(8000), back.Codecs[0].ClockRate)
	require.Equal(t, core.CodecPCMU, back.Codecs[1].Name)

	// the consumer's backchannel track, 0.5 s of PCMA at once
	codec := &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}
	track := core.NewReceiver(&core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{codec}}, codec)
	require.NoError(t, p.AddTrack(back, back.Codecs[0], track))

	// registering opens nothing at the camera, and a second track is no error
	other := core.NewReceiver(nil, codec)
	require.NoError(t, p.AddTrack(back, back.Codecs[0], other))
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, f.sessionEvents())

	src := tone(4000, true)
	feed(track, src)

	// 4000 samples at 8 kHz -> 7998 at 16 kHz (linear interpolation needs the
	// next sample) -> 15 whole blocks of 513 and a tail of 303 for the final block
	const whole = 15
	require.Eventually(t, func() bool { return len(f.blocks()) >= whole }, 3*time.Second, 5*time.Millisecond)
	blocks := f.blocks()[:whole]

	var out []int16
	var lastIndex byte
	for i, b := range blocks {
		block, rest := parseTalk(t, b.data)
		require.Empty(t, rest)
		require.Len(t, block, 4+256, "block_size = lengthPerEncoder/2 + 4")
		require.Zero(t, block[3])
		if i > 0 {
			require.Equal(t, lastIndex, block[2], "step index continues")
		}
		pcm16 := decodeIMALowFirst(block)
		require.Len(t, pcm16, 513)
		out = append(out, pcm16...)

		// the next block's header index is the state at the end of this one
		var s imaState
		s.pred, s.index = int32(pcm16[0]), int(block[2])
		for _, bb := range block[4:] {
			s.step(bb & 0xf)
			s.step(bb >> 4)
		}
		lastIndex = byte(s.index)
	}

	ref := lerp2(src, pcm.PCMAtoPCM, len(out))
	var sig, noise float64
	for i, got := range out {
		r := float64(ref[i])
		sig += r * r
		noise += (r - float64(got)) * (r - float64(got))
	}
	require.Len(t, out, whole*513)
	snr := 10 * math.Log10(sig/noise)
	require.Greater(t, snr, 20.0, "SNR %.1f dB", snr)

	// paced: 14 block times (14 * 32 ms) between the first and the last
	span := blocks[len(blocks)-1].at.Sub(blocks[0].at)
	require.GreaterOrEqual(t, span, 14*32*time.Millisecond*8/10, "sent in a burst")
	require.Less(t, span, 14*32*time.Millisecond*2)

	// the track is detached, nothing more comes: the tail leaves as one more
	// block, padded, and the reset waits until it has played
	track.Close()
	require.Eventually(t, func() bool { return len(f.blocks()) == whole+1 }, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset"}, f.sessionEvents())
	tail := f.blocks()[whole]
	reset := f.eventTime("reset", 0)
	blockTime := 513 * time.Second / 16000
	require.GreaterOrEqual(t, reset.Sub(tail.at), blockTime+talkDrain-10*time.Millisecond, "reset after the last block has played")

	// a new microphone can talk again
	track2 := core.NewReceiver(nil, codec)
	require.NoError(t, p.AddTrack(back, back.Codecs[0], track2))
	feed(track2, tone(1200, true))
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 3 }, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, p.Stop())
	require.Eventually(t, func() bool {
		ev := f.sessionEvents()
		return ev[len(ev)-1] == "reset"
	}, time.Second, 5*time.Millisecond, "Stop ends the session")
}

func TestProducerTalkIdleRestarts(t *testing.T) {
	fastIdle(t)
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	p := newTalkProducer(t, f)

	back := p.GetMedias()[2]
	codec := &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}
	track := core.NewReceiver(nil, codec)
	require.NoError(t, p.AddTrack(back, codec, track))
	feed(track, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, 3*time.Second, 5*time.Millisecond)

	// silence: the session is reset, a still connected track is not an error
	require.Eventually(t, func() bool { return len(f.sessionEvents()) >= 2 && f.sessionEvents()[1] == "reset" }, 3*time.Second, 5*time.Millisecond)

	// audio again: a new session starts by itself
	feed(track, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 2 }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"config:200", "reset", "config:200"}, f.sessionEvents())
	require.NoError(t, p.Stop())
}

func TestProducerTalkSenderClose(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400)
	p := newTalkProducer(t, f)

	back := p.GetMedias()[2]
	codec := &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}
	track := core.NewReceiver(nil, codec)
	require.NoError(t, p.AddTrack(back, codec, track))
	feed(track, tone(401, true))
	require.Eventually(t, func() bool { return len(f.blocks()) == 1 }, time.Second, time.Millisecond)

	p.hub.mu.Lock()
	sender := p.hub.talkers[0].sender
	p.hub.mu.Unlock()
	sender.Close()

	// graceful: the block (50 ms) plays out and gets its margin before the reset
	require.Eventually(t, func() bool { return len(f.sessionEvents()) == 2 }, time.Second, 5*time.Millisecond)
	gap := f.eventTime("reset", 0).Sub(f.blocks()[0].at)
	require.GreaterOrEqual(t, gap, 401*time.Second/8000+talkDrain-10*time.Millisecond)
	require.Less(t, gap, time.Second)

	// and the talker is gone with its sender
	require.Eventually(t, func() bool { return p.hub.count() == 0 }, time.Second, 5*time.Millisecond)
}

func TestProducerTalkPCMUAtCameraRate(t *testing.T) {
	f := newFakeCamera(t)
	f.talkAbility = adpcmAbility(8000, 400) // 200 data bytes, 401 samples, no resampling
	p := newTalkProducer(t, f)

	back := p.GetMedias()[2]
	codec := &core.Codec{Name: core.CodecPCMU, ClockRate: 8000}
	track := core.NewReceiver(nil, codec)
	require.NoError(t, p.AddTrack(back, codec, track))

	src := tone(802, false) // exactly two blocks
	feed(track, src)
	require.Eventually(t, func() bool { return len(f.blocks()) == 2 }, 3*time.Second, 5*time.Millisecond)

	var out []int16
	for _, b := range f.blocks() {
		block, _ := parseTalk(t, b.data)
		require.Len(t, block, 204)
		out = append(out, decodeIMALowFirst(block)...)
	}
	require.Len(t, out, 802)
	var sig, noise float64
	for i, got := range out {
		ref := float64(pcm.PCMUtoPCM(src[i]))
		sig += ref * ref
		noise += (ref - float64(got)) * (ref - float64(got))
	}
	require.Greater(t, 10*math.Log10(sig/noise), 20.0)

	// a codec the camera path cannot take
	bad := core.NewReceiver(nil, &core.Codec{Name: core.CodecOpus, ClockRate: 48000})
	require.Error(t, p.AddTrack(back, bad.Codec, bad))
}

// How internal/streams pairs a WebRTC/RTSP consumer's backchannel (recvonly)
// with the producer's sendonly media.
func TestTalkMediaMatchesConsumers(t *testing.T) {
	back := talkMedia()

	webrtc := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{
		{Name: core.CodecOpus, ClockRate: 48000, Channels: 2},
		{Name: core.CodecPCMU, ClockRate: 8000},
		{Name: core.CodecPCMA, ClockRate: 8000},
	}}
	prodCodec, consCodec := back.MatchMedia(webrtc)
	require.NotNil(t, prodCodec)
	require.NotNil(t, consCodec)
	require.Contains(t, []string{core.CodecPCMA, core.CodecPCMU}, prodCodec.Name)
	require.Equal(t, uint32(8000), prodCodec.ClockRate)

	opusOnly := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{
		{Name: core.CodecOpus, ClockRate: 48000, Channels: 2},
	}}
	prodCodec, _ = back.MatchMedia(opusOnly)
	require.Nil(t, prodCodec)
}
