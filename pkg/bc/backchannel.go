package bc

import (
	"encoding/json"
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

// These are variables only so that tests do not have to wait for them.
var (
	// talkIdle: an owner that gets no audio for this long ends its camera
	// session (a muted or disconnected microphone), and another talker that
	// does deliver audio may take over. The same track starts a new session
	// with its next packet.
	talkIdle = 2 * time.Second
	// talkRetire: a talker that was silent this long and does not own the
	// session is dropped when the next track is added. Consumers that went
	// away only detach their track, nothing else tells us.
	talkRetire = 30 * time.Second
	// talkDrain: after the last block was played the camera gets this much
	// time before the reset, which would cut off what is still playing.
	talkDrain = 100 * time.Millisecond
)

// probeTalk asks the camera for its talk format, best effort: a camera
// without talk (or one that does not answer) just gets no backchannel.
func probeTalk(client *Client) *TalkConfig {
	ability, err := client.getTalkAbility(talkAbilityTimeout)
	if err != nil {
		return nil
	}
	cfg, err := NewTalkConfig(ability, client.cfg.Channel)
	if err != nil {
		client.cfg.Log.Debugf("no backchannel: %v", err)
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

// talkHub holds the talkers of a producer, one per track added with AddTrack,
// and decides which of them owns the camera's single talk session.
//
// AddTrack only registers. The session is claimed by the first talker that
// delivers audio; the most recent audio wins: a talker whose owner has been
// silent for talkIdle takes the session over, the old session is ended
// gracefully first. go2rtc replays every microphone it ever attached on a
// reconnect, in order, and ignores errors, so a historical one must neither
// take the session nor make the current one fail.
type talkHub struct {
	p *Producer

	mu      sync.Mutex
	talkers []*talker
	owner   *talker
	closed  bool
}

func (h *talkHub) register(t *talker) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.talkers = append(h.talkers, t)
	return true
}

// remove drops a finished talker and frees what it held.
func (h *talkHub) remove(t *talker) {
	h.mu.Lock()
	for i, x := range h.talkers {
		if x == t {
			h.talkers = append(h.talkers[:i:i], h.talkers[i+1:]...)
			break
		}
	}
	if h.owner == t {
		h.owner = nil
		close(t.released)
	}
	h.mu.Unlock()
}

// claim makes t the owner of the camera session if nobody is, or if the owner
// has been silent for talkIdle (then it is asked to end its session first).
func (h *talkHub) claim(t *talker) bool {
	for {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return false
		}
		owner := h.owner
		if owner == nil || owner == t {
			h.owner = t
			t.released = make(chan struct{})
			h.mu.Unlock()
			return true
		}
		silent := time.Since(time.Unix(0, owner.lastAudio.Load())) >= talkIdle
		released := owner.released
		h.mu.Unlock()

		if !silent {
			return false // somebody is talking, this one waits for its turn
		}
		select {
		case owner.yield <- struct{}{}:
		default:
		}
		select {
		case <-released:
		case <-t.cancel:
			return false
		case <-time.After(5 * time.Second):
			return false
		}
	}
}

// release gives up ownership.
func (h *talkHub) release(t *talker) {
	h.mu.Lock()
	if h.owner == t {
		h.owner = nil
		close(t.released)
	}
	h.mu.Unlock()
}

// prune retires the talkers that were silent for talkRetire and are not the
// owner: their tracks are long gone.
func (h *talkHub) prune() {
	h.mu.Lock()
	var old []*talker
	for _, t := range h.talkers {
		if t != h.owner && time.Since(time.Unix(0, t.lastAudio.Load())) >= talkRetire {
			old = append(old, t)
		}
	}
	h.mu.Unlock()

	for _, t := range old {
		t.abort()
	}
}

// count is the number of registered talkers.
func (h *talkHub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.talkers)
}

// cancelAll stops everything at once and waits a moment for the workers to
// queue their resets. Nothing registers afterwards.
func (h *talkHub) cancelAll(wait time.Duration) {
	h.mu.Lock()
	h.closed = true
	all := append([]*talker(nil), h.talkers...)
	h.mu.Unlock()

	for _, t := range all {
		t.abort()
	}
	deadline := time.After(wait)
	for _, t := range all {
		select {
		case <-t.done:
		case <-deadline:
			return
		}
	}
}

// talker turns the G.711 of one consumer track into talk blocks. One worker
// goroutine owns all of its conversion state; other goroutines only signal it
// through channels.
type talker struct {
	h       *talkHub
	p       *Producer
	cfg     TalkConfig
	decode  func(byte) int16
	srcRate uint32
	sender  *core.Sender

	in       chan []byte   // G.711 payloads from the sender
	closeReq chan struct{} // the sender was closed: end gracefully
	cancel   chan struct{} // forced stop: abort at once
	yield    chan struct{} // another talker wants the session
	done     chan struct{} // the worker exited
	released chan struct{} // closed when this talker gives up the session (guarded by hub.mu)

	cancelOnce, closeOnce sync.Once
	lastAudio             atomic.Int64 // unix nanoseconds

	// worker only
	session *Talk
	enc     ADPCMEncoder
	rs      *resampler
	samples []int16
	next    time.Time // when the last block has played
	warned  bool
}

func (t *talker) abort() { t.cancelOnce.Do(func() { close(t.cancel) }) }

func (t *talker) requestClose() { t.closeOnce.Do(func() { close(t.closeReq) }) }

// AddTrack registers a microphone track for two-way audio. The camera's talk
// session starts with the first audio that arrives, see talkHub.
func (p *Producer) AddTrack(media *core.Media, _ *core.Codec, track *core.Receiver) error {
	if p.talkCfg == nil {
		return errors.New("bc: camera does not support talk")
	}
	if track.Codec.Name == core.CodecAny {
		// a probe (e.g. Frigate's ?microphone check) only asks whether talk
		// exists; it never sends audio, so there is nothing to attach
		return nil
	}

	t := &talker{
		h: p.hub, p: p, cfg: *p.talkCfg, srcRate: track.Codec.ClockRate,
		in: make(chan []byte, 64), closeReq: make(chan struct{}), cancel: make(chan struct{}),
		yield: make(chan struct{}, 1), done: make(chan struct{}),
	}
	t.lastAudio.Store(time.Now().UnixNano())
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

	if !p.hub.register(t) {
		return errors.New("bc: producer stopped")
	}
	p.hub.prune()

	t.sender = core.NewSender(media, track.Codec)
	t.sender.Handler = t.push
	t.sender.HandleRTP(track)

	go t.run()
	go func() {
		t.sender.Wait() // returns when the sender is closed
		t.requestClose()
	}()
	return nil
}

// push is the sender's handler. It never blocks; a worker that cannot keep
// up loses audio.
func (t *talker) push(packet *rtp.Packet) {
	if len(packet.Payload) == 0 {
		return
	}
	t.lastAudio.Store(time.Now().UnixNano())
	b := append([]byte(nil), packet.Payload...)
	select {
	case t.in <- b:
	default:
	}
}

func (t *talker) run() {
	defer close(t.done)
	defer func() {
		t.h.remove(t)
		t.sender.Close() // frees the sender, its queue and its wait goroutine
	}()

	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	clientDone := t.p.client.Done()

	for {
		var idleC <-chan time.Time
		var rejected <-chan struct{}
		if t.session != nil {
			idleC, rejected = idle.C, t.session.Rejected()
		}

		select {
		case <-t.cancel:
			t.abortSession()
			return
		case <-clientDone:
			t.dropSession() // nobody to reset, but the session object and its slot are freed
			return

		case <-t.closeReq:
			// drain what the sender queued before it was closed, then end
			for more := true; more; {
				select {
				case g := <-t.in:
					if !t.handleAudio(g, idle) {
						t.abortSession()
						return
					}
				default:
					more = false
				}
			}
			t.endSession(true)
			return

		case <-t.yield:
			t.endSession(true)
		case <-idleC:
			t.endSession(true)
		case <-rejected:
			t.p.client.cfg.Log.Warnf("talk: %v, ending the session", t.session.Err())
			t.endSession(true)

		case g := <-t.in:
			if !t.handleAudio(g, idle) {
				t.abortSession()
				return
			}
		}
	}
}

// handleAudio converts and sends one packet. It returns false when canceled.
func (t *talker) handleAudio(g []byte, idle *time.Timer) bool {
	if t.session == nil && !t.startSession() {
		return !t.canceled()
	}
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(talkIdle)

	pcm16 := make([]int16, len(g))
	for i, b := range g {
		pcm16[i] = t.decode(b)
	}
	if t.rs != nil {
		pcm16 = t.rs.process(pcm16)
	}
	t.samples = append(t.samples, pcm16...)

	perBlock := ADPCMSamples(t.session.DataBytes)
	for t.session != nil && len(t.samples) >= perBlock {
		chunk := t.samples[:perBlock]
		t.samples = t.samples[perBlock:]
		if !t.sendBlock(chunk) {
			if t.canceled() {
				return false
			}
			t.failSession() // the camera refused, the transport is fine
		}
	}
	return true
}

// canceled reports a forced stop or a transport that is gone.
func (t *talker) canceled() bool {
	select {
	case <-t.cancel:
		return true
	case <-t.p.client.Done():
		return true
	default:
		return false
	}
}

// startSession claims the camera session for this talker and opens it.
func (t *talker) startSession() bool {
	if !t.h.claim(t) {
		return false // an active talker owns it, or we are stopping
	}
	session, err := t.p.client.StartTalkCancel(t.cfg, t.cancel)
	if err != nil {
		t.h.release(t)
		if !t.warned && !t.canceled() {
			t.warned = true
			t.p.client.cfg.Log.Warnf("talk: %v", err)
		}
		return false
	}
	t.warned = false

	t.session = session
	t.enc = ADPCMEncoder{}
	t.samples = t.samples[:0]
	t.next = time.Time{}
	t.rs = nil
	if session.SampleRate != t.srcRate {
		t.rs = newResampler(t.srcRate, session.SampleRate)
	}
	return true
}

func (t *talker) blockDuration() time.Duration {
	return time.Duration(ADPCMSamples(t.session.DataBytes)) * time.Second / time.Duration(t.session.SampleRate)
}

// sleepUntil waits for the time, the forced stop or the transport. It
// reports false when it was cut short.
func (t *talker) sleepUntil(at time.Time) bool {
	d := time.Until(at)
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.cancel:
		return false
	case <-t.p.client.Done():
		return false
	}
}

// talkStale: a block that took this long to be written means the transport
// stalled; the audio queued meanwhile is old and is dropped rather than
// played as a burst afterwards.
var talkStale = 500 * time.Millisecond

// sendBlock encodes one block of samples (padded if short), sends it when the
// previous block has played and waits until the transport has written it. The
// playback deadline of the block counts from that write, so a stalled writer
// can neither make the blocks arrive in a burst nor let the final reset cut
// off what has just been delivered. Only one block is ever queued.
func (t *talker) sendBlock(samples []int16) bool {
	block, err := t.enc.EncodeBlock(samples, t.session.DataBytes)
	if err != nil {
		return true
	}
	if !t.sleepUntil(t.next) {
		return false
	}
	start := time.Now()
	written, err := t.session.SendNotify(block)
	if err != nil {
		if !errors.Is(err, ErrTalkStopped) {
			t.p.client.cfg.Log.Warnf("talk: %v", err)
		}
		return false
	}
	select {
	case <-written:
	case <-t.cancel:
		return false
	case <-t.p.client.Done():
		return false
	}

	now := time.Now()
	t.next = now.Add(t.blockDuration())
	t.p.talkSent.Add(int64(len(block)))
	if now.Sub(start) > talkStale {
		t.discardBacklog()
	}
	return true
}

// discardBacklog drops the microphone audio that piled up during a stall.
func (t *talker) discardBacklog() {
	t.samples = t.samples[:0]
	for {
		select {
		case <-t.in:
		default:
			return
		}
	}
}

// endSession ends the camera session. Gracefully the rest of the audio goes
// out as a final padded block and the reset waits until it has played (plus a
// margin), like neolink's talk_stream; a reset would cut it off. The session
// keeps the camera's slot until the reset has been answered.
func (t *talker) endSession(graceful bool) {
	s := t.session
	if s == nil {
		return
	}

	if graceful {
		ok := true
		if len(t.samples) > 0 && !s.Stopped() {
			chunk := t.samples
			t.samples = nil
			ok = t.sendBlock(chunk)
			if !ok && t.canceled() {
				t.abortSession()
				return
			}
		}
		// after a refused block there is nothing left to play
		if ok && !t.sleepUntil(t.next.Add(talkDrain)) {
			t.abortSession()
			return
		}
	}

	t.session = nil
	t.samples = t.samples[:0]
	_ = s.StopCancel(2*time.Second, t.cancel)
	t.h.release(t)
}

// failSession ends a session after an error on a healthy transport: the
// ordered stop, not the forced one, so the next session starts after the
// camera answered the reset.
func (t *talker) failSession() { t.endSession(false) }

// dropSession ends a session that failed.
func (t *talker) dropSession() {
	if s := t.session; s != nil {
		t.session = nil
		t.samples = t.samples[:0]
		s.StopAsync()
		t.h.release(t)
	}
}

// abortSession is the forced end: the reset is only queued.
func (t *talker) abortSession() { t.dropSession() }

// stopTalk cancels every talker and waits briefly for their resets to be
// queued. It never waits for the camera.
func (p *Producer) stopTalk(wait time.Duration) { p.hub.cancelAll(wait) }

// MarshalJSON adds the bytes sent for talk, which are counted atomically, to
// the connection info (core.Connection.Send is a plain int).
func (p *Producer) MarshalJSON() ([]byte, error) {
	c := p.Connection
	c.Send = int(p.talkSent.Load())
	return json.Marshal(&c)
}
