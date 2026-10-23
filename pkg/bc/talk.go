package bc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrTalkBusy    = errors.New("bc: a talk session is already active")
	ErrTalkStopped = errors.New("bc: talk session stopped")
)

// NewTalkConfig builds the msg 201 body from the camera's TalkAbility the way
// neolink does: the first duplex, the first audio stream mode (default
// followVideoStream) and the first audio config, copied as the camera sent it.
// Only ADPCM is supported.
func NewTalkConfig(a *TalkAbility, channel uint8) (*TalkConfig, error) {
	if a == nil || len(a.DuplexList) == 0 || len(a.AudioStreamModeList) == 0 || len(a.AudioConfigList) == 0 {
		return nil, errors.New("bc: camera does not support talk")
	}

	mode := "followVideoStream"
	if modes := a.AudioStreamModeList[0].AudioStreamMode; len(modes) > 0 {
		mode = modes[0]
	}

	cfg := &TalkConfig{
		Version:         XMLVersion,
		ChannelID:       channel,
		Duplex:          a.DuplexList[0].Duplex,
		AudioStreamMode: mode,
		AudioConfig:     a.AudioConfigList[0].AudioConfig,
	}
	if _, _, err := TalkFormat(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// TalkFormat returns the sample rate and the data bytes per ADPCM block (the
// block without its 4-byte header, lengthPerEncoder/2) the camera expects.
func TalkFormat(cfg *TalkConfig) (sampleRate uint32, dataBytes int, err error) {
	ac := cfg.AudioConfig
	if ac.AudioType != "adpcm" {
		return 0, 0, fmt.Errorf("bc: camera talk format %q is not supported, only adpcm", ac.AudioType)
	}
	dataBytes = int(ac.LengthPerEncoder) / 2
	if ac.SampleRate == 0 || dataBytes == 0 || dataBytes > MaxADPCMDataBytes {
		return 0, 0, fmt.Errorf("bc: bad talk format: rate %d, lengthPerEncoder %d", ac.SampleRate, ac.LengthPerEncoder)
	}
	return uint32(ac.SampleRate), dataBytes, nil
}

// TalkReset ends any talk session of the camera (msg 11).
func (c *Client) TalkReset() error { return c.talkReset(c.cfg.ReplyTimeout) }

func (c *Client) talkReset(timeout time.Duration) error {
	sb, err := c.subscribeNew(MsgTalkReset)
	if err != nil {
		return err
	}
	defer sb.close()

	ext := NewExtension()
	ext.ChannelID = &c.cfg.Channel
	err = c.send(&Message{MsgID: MsgTalkReset, ChannelID: c.cfg.Channel, MsgNum: sb.num, Class: ClassModern24, Extension: ext})
	if err != nil {
		return err
	}
	r, err := sb.recv(timeout)
	if err != nil {
		return err
	}
	if r.ResponseCode != 200 {
		return fmt.Errorf("bc: camera did not accept the talk stop, code %d", r.ResponseCode)
	}
	return nil
}

// Talk is a running talk session: blocks go out as msg 202 under one msg_num.
type Talk struct {
	c   *Client
	num uint16
	sub *sub

	SampleRate uint32 // of the PCM the blocks are made from
	DataBytes  int    // per block, without the 4-byte header

	stop    sync.Once
	stopped atomic.Bool
}

// StartTalk sends the talk config (msg 201). A 422 means another client is
// talking or a stale session exists: reset and retry once, as neolink does.
// Only one session per client at a time.
func (c *Client) StartTalk(cfg TalkConfig) (*Talk, error) {
	rate, dataBytes, err := TalkFormat(&cfg)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		err = c.errLocked()
	} else if c.talk != nil {
		err = ErrTalkBusy
	}
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	t := &Talk{c: c, SampleRate: rate, DataBytes: dataBytes}
	c.talk = t
	c.mu.Unlock()

	if err = t.start(&cfg); err != nil {
		t.release()
		return nil, err
	}
	return t, nil
}

func (t *Talk) start(cfg *TalkConfig) error {
	c := t.c

	sb, err := c.subscribeNew(MsgTalkConfig)
	if err != nil {
		return err
	}
	defer sb.close()

	ext := NewExtension()
	ext.ChannelID = &c.cfg.Channel
	msg := &Message{
		MsgID: MsgTalkConfig, ChannelID: c.cfg.Channel, MsgNum: sb.num, Class: ClassModern24,
		Extension: ext, Body: &Body{TalkConfig: cfg},
	}

	var r *Message
	for attempt := 0; ; attempt++ {
		if err = c.send(msg); err != nil {
			return err
		}
		if r, err = sb.recv(c.cfg.ReplyTimeout); err != nil {
			return fmt.Errorf("bc: talk config: %w", err)
		}
		if r.ResponseCode != 422 || attempt > 0 {
			break
		}
		if err = c.TalkReset(); err != nil {
			return fmt.Errorf("bc: talk reset: %w", err)
		}
	}
	if r.ResponseCode != 200 {
		return fmt.Errorf("bc: camera did not accept the talk config (audio format?), code %d", r.ResponseCode)
	}

	t.sub, err = c.subscribeNew(MsgTalk)
	if err == nil {
		t.num = t.sub.num
	}
	return err
}

func (t *Talk) release() {
	t.c.mu.Lock()
	if t.c.talk == t {
		t.c.talk = nil
	}
	t.c.mu.Unlock()
}

// Send transmits one ADPCM block made by ADPCMEncoder (msg 202). The caller
// paces it to real time, the camera buffers little.
func (t *Talk) Send(block []byte) error {
	if t.stopped.Load() {
		return ErrTalkStopped
	}

	// the camera acknowledges every block, an error code means it gave up
	for {
		select {
		case r := <-t.sub.ch:
			if r.ResponseCode >= 400 {
				return fmt.Errorf("bc: camera rejected talk audio, code %d", r.ResponseCode)
			}
			continue
		default:
		}
		break
	}

	frame, err := MarshalTalk(block)
	if err != nil {
		return err
	}
	ext := NewExtension()
	ext.ChannelID = &t.c.cfg.Channel
	ext.BinaryData = new(uint32)
	*ext.BinaryData = 1
	return t.c.send(&Message{
		MsgID: MsgTalk, ChannelID: t.c.cfg.Channel, MsgNum: t.num, Class: ClassModern24,
		Extension: ext, Binary: frame,
	})
}

// Stop ends the session (msg 11), idempotent. It does not wait long: the
// camera drops the session by itself when the connection closes.
func (t *Talk) Stop() (err error) {
	t.stop.Do(func() {
		t.stopped.Store(true)
		t.release()
		if t.sub != nil {
			t.sub.close()
		}
		select {
		case <-t.c.done:
		default:
			err = t.c.talkReset(2 * time.Second)
		}
	})
	return
}

// StopAsync ends the session without waiting for the camera: the reset is
// only queued. For shutdown paths that must not block.
func (t *Talk) StopAsync() {
	t.stop.Do(func() {
		t.stopped.Store(true)
		t.release()
		if t.sub != nil {
			t.sub.close()
		}
		ext := NewExtension()
		ext.ChannelID = &t.c.cfg.Channel
		_ = t.c.send(&Message{
			MsgID: MsgTalkReset, ChannelID: t.c.cfg.Channel, MsgNum: t.c.freeNum(), Class: ClassModern24, Extension: ext,
		})
	})
}

// Stopped reports whether the session was stopped.
func (t *Talk) Stopped() bool { return t.stopped.Load() }
