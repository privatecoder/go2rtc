package bc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

const talkAbilityTimeout = 3 * time.Second

// probeTalk asks the camera for its talk format, best effort: a camera
// without talk (or one that does not answer) just gets no backchannel.
func probeTalk(client *Client) *TalkConfig {
	ability, err := client.getTalkAbility(talkAbilityTimeout)
	if err != nil {
		return nil
	}
	cfg, err := NewTalkConfig(ability, client.cfg.Channel)
	if err != nil {
		client.cfg.Logf("no backchannel: %v", err)
		return nil
	}
	return cfg
}

// talkMedia is the sendonly media a camera with talk offers.
func talkMedia() *core.Media {
	return &core.Media{
		Kind:      core.KindAudio,
		Direction: core.DirectionSendonly,
		Codecs: []*core.Codec{
			{Name: core.CodecPCMA, ClockRate: 8000},
			{Name: core.CodecPCMU, ClockRate: 8000},
		},
	}
}

// talkIdle is how long a talk session may get no audio before it is reset.
// A consumer that disconnects only detaches its track, nothing tells us, and
// a muted browser sends nothing: the session is closed after the silence and
// started again by the next audio packet.
var talkIdle = 2 * time.Second

// talker turns the G.711 of one consumer track into talk blocks.
type talker struct {
	p       *Producer
	cfg     TalkConfig
	decode  func(byte) int16
	srcRate uint32
	idle    time.Duration

	closing atomic.Bool // set by Producer.Stop, checked without the mutex

	mu       sync.Mutex
	closed   bool
	talk     *Talk // nil while idle
	enc      ADPCMEncoder
	resample func([]int16) []int16
	samples  []int16
	next     time.Time // when the previous block has finished playing
	last     time.Time // last audio packet
	warned   bool
}

// AddTrack starts two-way audio: G.711 from the consumer is converted to PCM
// at the camera's rate, cut into ADPCM blocks of the size the camera asked
// for and sent paced to real time. Closing the sender ends the session.
func (p *Producer) AddTrack(media *core.Media, _ *core.Codec, track *core.Receiver) error {
	if p.talkCfg == nil {
		return errors.New("bc: camera does not support talk")
	}

	t := &talker{p: p, cfg: *p.talkCfg, srcRate: track.Codec.ClockRate, idle: talkIdle}
	if t.srcRate == 0 {
		t.srcRate = 8000
	}
	switch track.Codec.Name {
	case core.CodecPCMA:
		t.decode = pcm.PCMAtoPCM
	case core.CodecPCMU:
		t.decode = pcm.PCMUtoPCM
	default:
		return fmt.Errorf("bc: talk does not support codec %s", track.Codec.Name)
	}

	p.talkMu.Lock()
	defer p.talkMu.Unlock()

	// one session at a time, a talker that went quiet gives way
	if old := p.talker; old != nil {
		if old.active() {
			return ErrTalkBusy
		}
		old.shutdown()
	}

	t.mu.Lock()
	err := t.startLocked()
	t.mu.Unlock()
	if err != nil {
		return err
	}
	p.talker = t

	sender := core.NewSender(media, track.Codec)
	sender.Handler = t.handle
	sender.HandleRTP(track)
	p.Senders = append(p.Senders, sender)

	go func() {
		sender.Wait() // returns when the sender is closed
		t.shutdown()
	}()
	go t.watchIdle()
	return nil
}

func (t *talker) active() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.closed && t.talk != nil
}

// startLocked opens a talk session with fresh conversion state.
func (t *talker) startLocked() error {
	talk, err := t.p.client.StartTalk(t.cfg)
	if err != nil {
		return err
	}
	t.talk = talk
	t.enc = ADPCMEncoder{}
	t.samples = t.samples[:0]
	t.next = time.Time{}
	t.last = time.Now()

	t.resample = nil
	switch {
	case talk.SampleRate > t.srcRate:
		t.resample = pcm.Upsample(float32(talk.SampleRate) / float32(t.srcRate))
	case talk.SampleRate < t.srcRate:
		t.resample = pcm.Downsample(float32(t.srcRate) / float32(talk.SampleRate))
	}
	return nil
}

func (t *talker) stopLocked() {
	if t.talk != nil {
		_ = t.talk.Stop()
		t.talk = nil
	}
}

func (t *talker) shutdown() {
	t.mu.Lock()
	t.closed = true
	t.stopLocked()
	t.mu.Unlock()
}

func (t *talker) watchIdle() {
	tick := time.NewTicker(t.idle / 4)
	defer tick.Stop()
	for range tick.C {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		if t.talk != nil && time.Since(t.last) > t.idle {
			t.stopLocked()
		}
		t.mu.Unlock()
	}
}

// handle is the sender's handler, one packet at a time.
func (t *talker) handle(packet *rtp.Packet) {
	if t.closing.Load() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}
	if t.talk == nil {
		if err := t.startLocked(); err != nil {
			if !t.warned {
				t.warned = true
				t.p.client.cfg.Logf("talk: %v", err)
			}
			return
		}
		t.warned = false
	}
	t.last = time.Now()

	pcm16 := make([]int16, len(packet.Payload))
	for i, b := range packet.Payload {
		pcm16[i] = t.decode(b)
	}
	if t.resample != nil {
		pcm16 = t.resample(pcm16)
	}
	t.samples = append(t.samples, pcm16...)

	dataBytes := t.talk.DataBytes
	perBlock := ADPCMSamples(dataBytes)
	blockTime := time.Duration(perBlock) * time.Second / time.Duration(t.talk.SampleRate)

	for len(t.samples) >= perBlock {
		block, err := t.enc.EncodeBlock(t.samples[:perBlock], dataBytes)
		if err != nil {
			return
		}
		t.samples = t.samples[perBlock:]

		// the camera buffers little: a block goes out when the previous one
		// has played, never in a burst
		if wait := time.Until(t.next); wait > 0 {
			time.Sleep(wait)
		}
		now := time.Now()
		if err = t.talk.Send(block); err != nil {
			t.p.client.cfg.Logf("talk: %v", err)
			t.stopLocked()
			return
		}
		t.next = now.Add(blockTime)
		t.p.Send += len(block)
	}
}

// stopTalkAsync ends the talk session without waiting for anything: the
// talker is marked closed (atomically, its mutex may be held by a handler
// sleeping or talking to the camera) and the reset is only queued.
func (p *Producer) stopTalkAsync() {
	p.talkMu.Lock()
	t := p.talker
	p.talkMu.Unlock()
	if t != nil {
		t.closing.Store(true)
	}

	p.client.mu.Lock()
	talk := p.client.talk
	p.client.mu.Unlock()
	if talk != nil {
		talk.StopAsync()
	}
}
