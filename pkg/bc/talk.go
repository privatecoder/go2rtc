package bc

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTalkBusy    = errors.New("bc: a talk session is already active")
	ErrTalkStopped = errors.New("bc: talk session stopped")
)

// NewTalkConfig builds the msg 201 body from the camera's TalkAbility the way
// neolink does: the first duplex, the first audio stream mode (default
// followVideoStream) and the first audio config, copied as the camera sent it.
// The version attribute is empty, exactly as neolink's sender writes it (it
// builds the config with Default::default()); that is the known working form.
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
func (c *Client) TalkReset() error { return c.talkReset(c.cfg.ReplyTimeout, nil) }

func (c *Client) talkReset(timeout time.Duration, cancel <-chan struct{}) error {
	sb, err := c.subscribeNew(MsgTalkReset)
	if err != nil {
		return err
	}
	defer sb.close()

	if err = c.sendReset(sb.num); err != nil {
		return err
	}
	r, err := sb.recvCancel(timeout, cancel)
	if err != nil {
		return err
	}
	if r.ResponseCode != 200 {
		return fmt.Errorf("bc: camera did not accept the talk stop, code %d", r.ResponseCode)
	}
	return nil
}

func (c *Client) sendReset(num uint16) error {
	ext := NewExtension()
	ext.ChannelID = &c.cfg.Channel
	return c.send(&Message{MsgID: MsgTalkReset, ChannelID: c.cfg.Channel, MsgNum: num, Class: ClassModern24, Extension: ext})
}

type talkState uint8

const (
	talkActive talkState = iota
	talkClosing
	talkClosed
)

// Talk is a running talk session: blocks go out as msg 202 under one msg_num.
//
// The session owns the camera's single talk slot from the accepted config
// (msg 201) until the reset (msg 11) has been answered, or the transport
// failed. A successor's config therefore always follows the predecessor's
// reset on the wire, and no block follows the reset of its own session: Send
// and Stop are serialized by one mutex that is never held across a wait.
type Talk struct {
	c   *Client
	num uint16
	sub *sub

	SampleRate uint32 // of the PCM the blocks are made from
	DataBytes  int    // per block, without the 4-byte header

	mu        sync.Mutex
	state     talkState
	ended     chan struct{} // closed when the slot is free again
	endOnce   sync.Once
	rejected  chan struct{} // closed when the camera answers a block with an error
	rejectErr error
}

// StartTalk sends the talk config (msg 201). A 422 means another client is
// talking or a stale session exists: reset and retry once, as neolink does.
// Only one session per client at a time; while the previous one is still
// being reset the call waits for the reset to finish.
func (c *Client) StartTalk(cfg TalkConfig) (*Talk, error) { return c.StartTalkCancel(cfg, nil) }

// StartTalkCancel is StartTalk that gives up promptly when cancel is closed.
func (c *Client) StartTalkCancel(cfg TalkConfig, cancel <-chan struct{}) (*Talk, error) {
	rate, dataBytes, err := TalkFormat(&cfg)
	if err != nil {
		return nil, err
	}
	t := &Talk{
		c: c, SampleRate: rate, DataBytes: dataBytes,
		ended: make(chan struct{}), rejected: make(chan struct{}),
	}

	for deadline := time.After(c.cfg.ReplyTimeout); ; {
		c.mu.Lock()
		if c.closed {
			err = c.errLocked()
			c.mu.Unlock()
			return nil, err
		}
		prev := c.talk
		if prev == nil {
			c.talk = t
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()

		// a session that is being reset keeps the slot until the camera answered
		if !prev.isClosing() {
			return nil, ErrTalkBusy
		}
		select {
		case <-prev.ended:
		case <-cancel:
			return nil, ErrCanceled
		case <-c.done:
			return nil, c.Err()
		case <-deadline:
			return nil, ErrTalkBusy
		}
	}

	if err = t.start(&cfg, cancel); err != nil {
		t.finish()
		return nil, err
	}
	go t.watchAcks()
	return t, nil
}

func (t *Talk) isClosing() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == talkClosing
}

func (t *Talk) start(cfg *TalkConfig, cancel <-chan struct{}) error {
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
		if r, err = sb.recvCancel(c.cfg.ReplyTimeout, cancel); err != nil {
			// the camera may have accepted it: do not leave it with a session nobody uses
			_ = c.sendReset(c.freeNum())
			return fmt.Errorf("bc: talk config: %w", err)
		}
		if r.ResponseCode != 422 || attempt > 0 {
			break
		}
		if err = c.talkReset(c.cfg.ReplyTimeout, cancel); err != nil {
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

// finish frees the camera slot for a successor. Once.
func (t *Talk) finish() {
	t.endOnce.Do(func() {
		t.mu.Lock()
		t.state = talkClosed
		t.mu.Unlock()

		if t.sub != nil {
			t.sub.close()
		}
		t.c.mu.Lock()
		if t.c.talk == t {
			t.c.talk = nil
		}
		t.c.mu.Unlock()
		close(t.ended)
	})
}

// watchAcks looks at the camera's answers to the blocks: an error code ends
// the session (Rejected), a missing answer is not an error, blocks are paced.
func (t *Talk) watchAcks() {
	for {
		select {
		case r := <-t.sub.ch:
			if r.ResponseCode >= 400 {
				t.mu.Lock()
				if t.rejectErr == nil {
					t.rejectErr = fmt.Errorf("bc: camera rejected talk audio, code %d", r.ResponseCode)
					close(t.rejected)
				}
				t.mu.Unlock()
			}
		case <-t.ended:
			return
		case <-t.c.done:
			// the transport is gone, nobody will answer a reset: free the slot
			t.finish()
			return
		}
	}
}

// Rejected is closed when the camera answered a block with an error code.
func (t *Talk) Rejected() <-chan struct{} { return t.rejected }

// Err is why the camera rejected the audio, nil if it did not.
func (t *Talk) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rejectErr
}

// Ended is closed once the camera's talk slot is free again.
func (t *Talk) Ended() <-chan struct{} { return t.ended }

// Send transmits one ADPCM block made by ADPCMEncoder (msg 202): exactly the
// negotiated 4 + DataBytes bytes with a valid step index. The caller paces
// it, the camera buffers little. Nothing is sent when it returns an error.
func (t *Talk) Send(block []byte) error {
	_, err := t.SendNotify(block)
	return err
}

// SendNotify is Send that also returns a channel which is closed when the
// transport has actually written the block, so that a sender can pace on the
// real output and not on the queue. It stays open if the client fails first.
func (t *Talk) SendNotify(block []byte) (<-chan struct{}, error) {
	if len(block) != adpcmHeaderLen+t.DataBytes {
		return nil, fmt.Errorf("bc: talk block is %d bytes, the camera asked for %d", len(block), adpcmHeaderLen+t.DataBytes)
	}
	if block[2] > 88 {
		return nil, fmt.Errorf("bc: talk block with step index %d", block[2])
	}
	frame, err := MarshalTalk(block)
	if err != nil {
		return nil, err
	}
	ext := NewExtension()
	ext.ChannelID = &t.c.cfg.Channel
	ext.BinaryData = new(uint32)
	*ext.BinaryData = 1

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != talkActive {
		return nil, ErrTalkStopped
	}
	if t.rejectErr != nil {
		return nil, t.rejectErr
	}
	return t.c.sendNotify(&Message{
		MsgID: MsgTalk, ChannelID: t.c.cfg.Channel, MsgNum: t.num, Class: ClassModern24,
		Extension: ext, Binary: frame,
	})
}

// Stop ends the session (msg 11) and waits for the camera's answer, at most
// 2 s. Idempotent: a second call waits for the first to finish.
func (t *Talk) Stop() error { return t.StopCancel(2*time.Second, nil) }

// StopCancel is Stop with its own wait budget and a cancel channel. The
// reset is queued while the session mutex is held, so it follows every block
// queued before; no block can follow it. The slot stays taken until the
// camera answered (or the budget, cancel or the transport ended the wait).
func (t *Talk) StopCancel(wait time.Duration, cancel <-chan struct{}) error {
	t.mu.Lock()
	if t.state != talkActive {
		t.mu.Unlock()
		select {
		case <-t.ended:
		case <-time.After(wait):
		case <-cancel:
		}
		return nil
	}
	t.state = talkClosing

	sb, err := t.c.subscribeNew(MsgTalkReset)
	if err == nil {
		err = t.c.sendReset(sb.num)
	}
	t.mu.Unlock()
	if err != nil {
		t.finish()
		return err
	}
	defer sb.close()

	r, err := sb.recvCancel(wait, cancel)
	t.finish()
	if err == nil && r.ResponseCode != 200 {
		err = fmt.Errorf("bc: camera did not accept the talk stop, code %d", r.ResponseCode)
	}
	return err
}

// StopAsync ends the session without waiting for the camera: the reset is
// only queued (after the blocks already queued) and the slot is released at
// once. For forced shutdown, when the transport is going away anyway.
func (t *Talk) StopAsync() {
	t.mu.Lock()
	was := t.state
	t.state = talkClosed
	if was == talkActive {
		_ = t.c.sendReset(t.c.freeNum())
	}
	t.mu.Unlock()
	t.finish()
}

// Stopped reports whether the session no longer accepts blocks.
func (t *Talk) Stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state != talkActive
}
