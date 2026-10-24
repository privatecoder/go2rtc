package bc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

// These are variables only so that tests do not have to wait for them.
var (
	maxMove      = 10 * time.Second       // longest a continuous move runs without a Stop
	controlIdle  = 60 * time.Second       // a control session closes after this long unused
	zoomInterval = 200 * time.Millisecond // between two steps of a continuous zoom

	// An unacknowledged Stop is sent again after these waits (the last one
	// repeats) until stopGiveUp has passed since it was wanted.
	stopBackoff = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}
	stopGiveUp  = 60 * time.Second

	stopWait = 3 * time.Second  // Stop returns after this at the latest, the retries go on
	moveWait = 15 * time.Second // Move waits this long for its own command at the most
	capsWait = 3 * time.Second  // Move waits this long for the zoom range
)

const (
	ptzDeadZone  = 0.05
	ptzMaxSpeed  = 64
	ptzMaxPreset = 63
	zoomSteps    = 20 // a full zoom range takes this many steps at full speed
)

// CameraKey identifies the camera of a bc:// URL: host and channel, the port
// and the stream do not matter. The UID and the IP of one camera are two keys.
func CameraKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Hostname() == "" {
		return "", errors.New("bc: no host in url")
	}
	var channel uint8
	if s := u.Query().Get("channel"); s != "" {
		channel = core.ParseByte(s)
	}
	return strings.ToLower(u.Hostname()) + "/" + strconv.Itoa(int(channel)), nil
}

// registry of the clients of running producers, by camera. PTZ commands use
// them instead of logging in again. registryChange is closed and replaced
// whenever a client is added, which wakes the PTZ calls waiting for a session.
var (
	registryMu     sync.Mutex
	registry       = map[string][]*Client{}
	registryChange = make(chan struct{})
)

func registerClient(key string, c *Client) {
	registryMu.Lock()
	registry[key] = append(registry[key], c)
	close(registryChange)
	registryChange = make(chan struct{})
	registryMu.Unlock()

	go func() {
		<-c.Done()
		registryMu.Lock()
		defer registryMu.Unlock()
		list := registry[key]
		for i, x := range list {
			if x == c {
				list = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(registry, key)
		} else {
			registry[key] = list
		}
	}()
}

func registryWatch() <-chan struct{} {
	registryMu.Lock()
	defer registryMu.Unlock()
	return registryChange
}

// activeClient is the most recently registered live client of the camera.
func activeClient(key string) *Client {
	registryMu.Lock()
	defer registryMu.Unlock()
	list := registry[key]
	for i := len(list) - 1; i >= 0; i-- {
		select {
		case <-list[i].Done():
		default:
			return list[i]
		}
	}
	return nil
}

var (
	ptzMu    sync.Mutex
	ptzByKey = map[string]*PTZ{}
)

// PTZFor returns the controller of the camera in a bc:// URL, one per camera
// for the life of the process. The URL of the first caller is used if a
// control session has to be opened.
func PTZFor(rawURL string) (*PTZ, error) {
	key, err := CameraKey(rawURL)
	if err != nil {
		return nil, err
	}
	ptzMu.Lock()
	defer ptzMu.Unlock()
	p := ptzByKey[key]
	if p == nil {
		p = newPTZ(key, rawURL)
		ptzByKey[key] = p
	}
	return p, nil
}

// Velocity is an ONVIF velocity, -1..1 per axis, +Tilt is up and +Zoom is in.
type Velocity struct {
	Pan, Tilt, Zoom     float64
	HasPanTilt, HasZoom bool
}

// PTZ controls one camera. It sends over the client of a running stream of
// that camera, and only if there is none over a session of its own that closes
// when idle. It never takes part in streaming.
//
// Pan/tilt commands go through one loop. Move, Stop and the safety deadline
// only change the wanted state under moveMu (no I/O there) and wake the loop,
// which sends the latest wanted command: a command replaced before it was sent
// is never sent, so a Stop is never followed by an older Move. A camera that
// may be moving is stopped until a Stop is acknowledged.
type PTZ struct {
	key, rawURL string
	open        func() (*Client, error) // opens a control session, p.login unless a test replaces it

	capsMu sync.Mutex
	caps   *PTZCaps
	flight *capsFlight

	mu     sync.Mutex // control session
	ctrl   *Client
	idle   *time.Timer
	used   time.Time
	dialer *dialFlight

	presetMu sync.Mutex

	moveMu     sync.Mutex // the state below
	desired    *panCmd    // next command for the loop, nil if none
	kick       chan struct{}
	looping    bool
	mayMove    bool      // the camera may be moving: a Move was accepted, no Stop acknowledged since
	stopWanted bool      // a Stop must be sent until it is acknowledged
	stopSince  time.Time // when the Stop was first wanted
	retryAt    time.Time
	fails      int
	gen        uint64      // the move the deadline belongs to
	seq        uint64      // order of Move, Stop and GotoPreset calls, taken before a Move validates
	motion     *Client     // where the motion commands went while the camera is not settled
	timer      *time.Timer // the safety deadline
	stepper    *zoomStepper

	noNativeZoom bool        // the camera refused ZoomInc/ZoomDec once, zoom is stepped from then on
	nativeMaybe  bool        // a native zoom may be running: set by Move, cleared by an acknowledged Stop or preset
	fallback     *stepsAfter // lens steps that wait for the Stop of a refused native zoom
}

func newPTZ(key, rawURL string) *PTZ {
	p := &PTZ{key: key, rawURL: rawURL, kick: make(chan struct{})}
	p.open = p.login
	return p
}

// stepsAfter is the stepper of a refused native zoom, started when the Stop
// that goes first is acknowledged.
type stepsAfter struct {
	seq  uint64
	zoom float64
	caps PTZCaps
}

// panCmd is a pan/tilt command for the loop.
type panCmd struct {
	dir   Direction
	speed float32

	isPreset bool // go to preset, not a pan/tilt command
	preset   uint8

	// of a native zoom, for the stepper that takes over if the camera refuses it
	seq  uint64
	zoom float64
	caps PTZCaps

	done chan error // buffered, the result of the command
	once sync.Once
}

func newPanCmd(dir Direction, speed float32) *panCmd {
	return &panCmd{dir: dir, speed: speed, done: make(chan error, 1)}
}

func (c *panCmd) isStop() bool { return !c.isPreset && c.dir == DirStop }

func (c *panCmd) isZoom() bool { return !c.isPreset && (c.dir == DirZoomInc || c.dir == DirZoomDec) }

func (c *panCmd) complete(err error) { c.once.Do(func() { c.done <- err }) }

type capsFlight struct {
	done chan struct{}
	err  error
}

type dialFlight struct {
	done chan struct{}
	c    *Client
	err  error
}

// Caps are cached after the first success, concurrent callers share one fetch.
// If ctx ends first the fetch carries on and fills the cache.
func (p *PTZ) Caps(ctx context.Context) (PTZCaps, error) {
	p.capsMu.Lock()
	if p.caps != nil {
		c := *p.caps
		p.capsMu.Unlock()
		return c, nil
	}
	fl := p.flight
	if fl == nil {
		fl = &capsFlight{done: make(chan struct{})}
		p.flight = fl
		go func() {
			var caps PTZCaps
			err := p.do(context.Background(), func(c *Client, _ <-chan struct{}) (err error) {
				caps, err = c.PTZCaps()
				return
			})
			p.capsMu.Lock()
			if err == nil {
				p.caps = &caps
			}
			fl.err = err
			p.flight = nil
			p.capsMu.Unlock()
			close(fl.done)
		}()
	}
	p.capsMu.Unlock()

	select {
	case <-fl.done:
	case <-ctx.Done():
		return PTZCaps{}, ctx.Err()
	}
	if fl.err != nil {
		return PTZCaps{}, fl.err
	}
	p.capsMu.Lock()
	defer p.capsMu.Unlock()
	return *p.caps, nil
}

// do runs fn on a client of the camera, cancel ends when ctx does. A client
// that died under the call is replaced once.
func (p *PTZ) do(ctx context.Context, fn func(c *Client, cancel <-chan struct{}) error) error {
	for attempt := 0; ; attempt++ {
		c, control, err := p.acquire(ctx)
		if err != nil {
			return err
		}
		err = fn(c, ctx.Done())
		if err == nil || attempt > 0 {
			return err
		}
		select {
		case <-c.Done():
			if control {
				p.drop(c)
			}
		default:
			return err
		}
	}
}

// acquire is the client of a running stream or else the control session. A
// stream that starts meanwhile wins, also while a control session is being
// opened (that one finishes and expires unused).
func (p *PTZ) acquire(ctx context.Context) (c *Client, control bool, err error) {
	for {
		changed := registryWatch() // before the lookup, so a registration is not missed
		if c = activeClient(p.key); c != nil {
			return c, false, nil
		}

		p.mu.Lock()
		if p.ctrl != nil {
			select {
			case <-p.ctrl.Done():
				p.dropLocked(p.ctrl)
			default:
				c = p.ctrl
				p.touchLocked()
				p.mu.Unlock()
				return c, true, nil
			}
		}
		fl := p.dialer
		if fl == nil {
			fl = &dialFlight{done: make(chan struct{})}
			p.dialer = fl
			go p.openControl(fl)
		}
		p.mu.Unlock()

		select {
		case <-fl.done:
			if c = activeClient(p.key); c != nil {
				return c, false, nil
			}
			if fl.err != nil {
				return nil, false, fl.err
			}
		case <-changed:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

func (p *PTZ) openControl(fl *dialFlight) {
	c, err := p.open()

	p.mu.Lock()
	if err == nil {
		p.ctrl = c
		p.touchLocked()
	}
	fl.c, fl.err = c, err
	p.dialer = nil
	p.mu.Unlock()
	close(fl.done)
}

// login opens a session without a stream.
func (p *PTZ) login() (*Client, error) {
	conn, cfg, _, _, err := connect(p.rawURL, nil)
	if err != nil {
		return nil, err
	}
	c := NewClient(conn, cfg)
	if _, err = c.Login(); err != nil {
		_ = c.Close()
		return nil, err
	}
	c.cfg.Log.Debugf("ptz control session opened")
	return c, nil
}

// touchLocked marks the control session used and arms the idle close.
func (p *PTZ) touchLocked() {
	p.used = time.Now()
	if p.idle == nil {
		c := p.ctrl
		p.idle = time.AfterFunc(controlIdle, func() { p.expire(c) })
	} else {
		p.idle.Reset(controlIdle)
	}
}

func (p *PTZ) expire(c *Client) {
	p.mu.Lock()
	if p.ctrl != c {
		p.mu.Unlock()
		return
	}
	if wait := controlIdle - time.Since(p.used); wait > 0 {
		p.idle.Reset(wait)
		p.mu.Unlock()
		return
	}
	p.dropLocked(c)
	p.mu.Unlock()
}

func (p *PTZ) drop(c *Client) {
	p.mu.Lock()
	p.dropLocked(c)
	p.mu.Unlock()
}

func (p *PTZ) dropLocked(c *Client) {
	if p.ctrl != c {
		return
	}
	p.ctrl = nil
	if p.idle != nil {
		p.idle.Stop()
		p.idle = nil
	}
	go c.Close()
}

// Move starts a continuous move that ends with Stop, with the timeout or, if
// that is zero or longer than maxMove, after maxMove. It replaces the move
// before it. The safety deadline runs from here, whatever the camera or the
// session do after. It returns the result of its own pan/tilt command, nil if
// a newer command replaced it before it was sent.
func (p *PTZ) Move(v Velocity, timeout time.Duration) error {
	if timeout <= 0 || timeout > maxMove {
		timeout = maxMove
	}
	pan, tilt, zoom := unit(v.Pan), unit(v.Tilt), unit(v.Zoom)

	// the order of the calls is the order they were made, whatever validation takes
	p.moveMu.Lock()
	p.seq++
	mySeq := p.seq
	p.moveMu.Unlock()

	// before anything changes, a camera without zoom must not half-do the move
	var caps PTZCaps
	wantZoom := v.HasZoom && zoom != 0
	if wantZoom {
		ctx, cancel := context.WithTimeout(context.Background(), capsWait)
		var err error
		caps, err = p.Caps(ctx)
		cancel()
		if err == nil && !caps.Zoom {
			err = errors.New("bc: camera has no zoom")
		}
		if err != nil {
			return err
		}
	}

	deadline := time.Now().Add(timeout)

	p.moveMu.Lock()
	if p.seq != mySeq {
		p.moveMu.Unlock() // a Stop or a newer call came in meanwhile
		return nil
	}
	p.gen++
	prev := p.cancelStepperLocked()

	dir, speed := DirStop, float32(0)
	if v.HasPanTilt {
		dir, speed = direction(pan, tilt)
	}
	// the lens has its own continuous zoom, the steps are for combined moves and old cameras
	native := wantZoom && dir == DirStop && !p.noNativeZoom
	if native {
		dir, speed = zoomDirection(zoom)
		p.nativeMaybe = true
	}

	var cmd *panCmd
	// a pan or tilt still running from the move before would lose its deadline
	if v.HasPanTilt || p.mayMove || native {
		cmd = newPanCmd(dir, speed)
		cmd.seq, cmd.zoom, cmd.caps = mySeq, zoom, caps
		if dir == DirStop {
			p.wantStopLocked()
		} else {
			p.mayMove, p.stopWanted, p.stopSince = true, false, time.Time{}
		}
		p.setDesiredLocked(cmd)
	}
	if p.mayMove || wantZoom {
		p.armLocked(deadline)
	}
	if wantZoom && !native {
		p.stepper = p.startStepper(zoom, caps, prev)
	}
	p.moveMu.Unlock()

	if cmd == nil {
		return nil
	}
	t := time.NewTimer(moveWait)
	defer t.Stop()
	select {
	case err := <-cmd.done:
		return err
	case <-t.C:
		return errors.New("bc: ptz move: no result in time")
	}
}

// Stop ends the pan/tilt and/or the zoom of the current move. A pan/tilt Stop
// is sent even if no move is known, it may have been started by someone else.
// It returns after the first attempt (or stopWait), an unacknowledged Stop is
// repeated in the background.
func (p *PTZ) Stop(panTilt, zoom bool) error {
	p.moveMu.Lock()
	p.seq++
	p.fallback = nil
	// one motor command channel: the stop that halts a pan halts a native zoom
	panTilt = panTilt || (zoom && (!p.noNativeZoom || p.nativeMaybe))
	s := p.cancelStepperLocked() // whatever the flags, a Stop ends the lens steps too
	var cmd *panCmd
	if panTilt {
		cmd = newPanCmd(DirStop, 0)
		p.wantStopLocked()
		p.setDesiredLocked(cmd)
	} else if !p.mayMove && p.stepper == nil {
		p.gen++
		p.cancelTimerLocked()
	}
	p.moveMu.Unlock()

	var err error
	if cmd != nil {
		t := time.NewTimer(stopWait)
		select {
		case err = <-cmd.done:
		case <-t.C:
			err = errors.New("bc: ptz stop not acknowledged yet, retrying")
		}
		t.Stop()
	}
	if s != nil {
		<-s.done // its requests are cancelled, this is quick
	}
	return err
}

// wantStopLocked starts the obligation to stop the camera.
func (p *PTZ) wantStopLocked() {
	p.mayMove = true
	if !p.stopWanted {
		p.stopWanted = true
		p.stopSince = time.Now()
		p.retryAt = time.Time{}
		p.fails = 0
	}
}

// setDesiredLocked makes cmd the next command, one not yet sent is dropped.
func (p *PTZ) setDesiredLocked(cmd *panCmd) {
	if old := p.desired; old != nil {
		old.complete(nil)
	}
	p.desired = cmd
	close(p.kick) // ends the wait of a command in flight
	p.kick = make(chan struct{})
	if !p.looping {
		p.looping = true
		go p.run()
	}
}

func (p *PTZ) cancelTimerLocked() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
}

func (p *PTZ) armLocked(deadline time.Time) {
	p.cancelTimerLocked()
	gen := p.gen
	p.timer = time.AfterFunc(time.Until(deadline), func() { p.expireMove(gen) })
}

// expireMove is the safety stop: the wanted state becomes stop.
func (p *PTZ) expireMove(gen uint64) {
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	if p.gen != gen {
		return
	}
	p.cancelStepperLocked()
	if p.mayMove && !p.stopWanted {
		Log.Filled().Debugf("ptz: no Stop in time, stopping")
		p.wantStopLocked()
		p.setDesiredLocked(newPanCmd(DirStop, 0))
	}
}

// run is the command loop, it ends when there is nothing to send or retry.
func (p *PTZ) run() {
	for {
		p.moveMu.Lock()
		cmd := p.desired
		p.desired = nil
		kick := p.kick
		if cmd == nil {
			if !p.stopWanted || !p.mayMove {
				p.looping = false
				p.moveMu.Unlock()
				return
			}
			if wait := time.Until(p.retryAt); wait > 0 {
				p.moveMu.Unlock()
				t := time.NewTimer(wait)
				select {
				case <-t.C:
				case <-kick:
				}
				t.Stop()
				continue
			}
			if time.Since(p.stopSince) > stopGiveUp {
				p.fallback = nil
				p.mayMove, p.stopWanted = false, false // the session stays pinned: older motion may still be queued on it
				if p.stepper == nil {                  // a running zoom keeps its deadline
					p.cancelTimerLocked()
				}
				p.moveMu.Unlock()
				Log.Filled().Warnf("ptz: giving up stopping the camera, no Stop was acknowledged")
				continue
			}
			cmd = newPanCmd(DirStop, 0) // a retry
		}
		p.moveMu.Unlock()
		p.send(cmd, kick)
	}
}

// send sends one command over whatever session there is. kick is closed when
// a newer command is wanted, which ends the waits for the session and, for a
// Stop, for the reply.
func (p *PTZ) send(cmd *panCmd, kick <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-kick:
			cancel()
		case <-ctx.Done():
		}
	}()

	c, err := p.motionClient(ctx)
	if err != nil {
		if ctx.Err() != nil {
			cmd.complete(nil) // replaced
			return
		}
		p.failed(cmd, err)
		return
	}

	p.moveMu.Lock()
	replaced := p.desired != nil
	p.moveMu.Unlock()
	if replaced {
		cmd.complete(nil)
		return
	}

	var body *Body
	var id uint32
	if cmd.isPreset {
		id, body = MsgPTZPreset, c.presetBody(Preset{ID: cmd.preset, Command: "toPos"})
	} else {
		id, body = MsgPTZControl, c.ptzMoveBody(cmd.dir, cmd.speed)
	}
	call, err := c.ptzStart(id, body)
	if err != nil {
		p.failed(cmd, err)
		return
	}
	// until settled every motion command goes over this client: another
	// session's writer could deliver an older command after a newer one
	p.moveMu.Lock()
	p.motion = c
	p.moveMu.Unlock()

	if !cmd.isPreset && cmd.dir != DirStop {
		// not waited for here: a Stop must not queue behind the reply
		go func() {
			_, err := call.wait(nil)
			if err != nil {
				Log.Filled().Debugf("ptz: move not acknowledged: %v", err)
				var ce *codeError
				if cmd.isZoom() && errors.As(err, &ce) {
					err = p.stepInstead(cmd, err)
				}
			}
			cmd.complete(err)
		}()
		return
	}

	_, err = call.wait(ctx.Done())
	switch {
	case errors.Is(err, ErrCanceled):
		if cmd.isPreset {
			// sent, and a Stop or newer command took over before the answer
			cmd.complete(errors.New("bc: ptz preset interrupted before it was acknowledged"))
		} else {
			cmd.complete(nil) // replaced, the newer command goes on from here
		}
	case err != nil:
		p.failed(cmd, err)
	default:
		p.settled(cmd)
	}
}

// stepInstead is the fallback of a camera that refuses native zoom: it is
// remembered, and the move goes on as steps if it is still the wanted one.
// Otherwise err stays the result.
func (p *PTZ) stepInstead(cmd *panCmd, err error) error {
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	p.noNativeZoom = true
	if p.seq != cmd.seq || p.desired != nil || !p.mayMove || p.stopWanted {
		return err // replaced, stopped or expired meanwhile
	}
	// the command that was refused replaced a move that may still run: stop
	// it first, the steps follow once that Stop is acknowledged (settled)
	p.wantStopLocked()
	p.setDesiredLocked(newPanCmd(DirStop, 0))
	p.fallback = &stepsAfter{seq: cmd.seq, zoom: cmd.zoom, caps: cmd.caps}
	return nil
}

// motionClient is the client the camera's unsettled motion was last commanded
// on if it is alive, else whatever session there is.
func (p *PTZ) motionClient(ctx context.Context) (*Client, error) {
	p.moveMu.Lock()
	m := p.motion
	p.moveMu.Unlock()
	if m != nil {
		select {
		case <-m.Done():
			p.moveMu.Lock()
			if p.motion == m {
				p.motion = nil
			}
			p.moveMu.Unlock()
		default:
			p.mu.Lock()
			if p.ctrl == m {
				p.touchLocked() // in use, not idle
			}
			p.mu.Unlock()
			return m, nil
		}
	}
	c, _, err := p.acquire(ctx)
	return c, err
}

func (p *PTZ) failed(cmd *panCmd, err error) {
	Log.Filled().Debugf("ptz: command not sent: %v", err)
	p.moveMu.Lock()
	switch {
	case cmd.isStop():
		p.retryAt = time.Now().Add(stopBackoff[min(p.fails, len(stopBackoff)-1)])
		p.fails++
	case cmd.isPreset && p.mayMove && !p.stopWanted && p.desired == nil:
		// the preset did not happen: stop the move it replaced now, not at its deadline
		p.wantStopLocked()
		p.setDesiredLocked(newPanCmd(DirStop, 0))
	}
	p.moveMu.Unlock()
	cmd.complete(err)
}

// settled is an acknowledged Stop or GotoPreset (a preset move is finite): the
// camera is settled unless a newer command is already wanted.
func (p *PTZ) settled(cmd *panCmd) {
	p.moveMu.Lock()
	if p.desired == nil {
		p.mayMove, p.stopWanted, p.stopSince, p.fails = false, false, time.Time{}, 0
		p.nativeMaybe = false
		p.motion = nil
		fb := p.fallback
		p.fallback = nil
		if fb != nil && fb.seq == p.seq && cmd.isStop() {
			p.stepper = p.startStepper(fb.zoom, fb.caps, nil) // the deadline stays
		}
		if p.stepper == nil {
			p.cancelTimerLocked()
		}
	}
	p.moveMu.Unlock()
	cmd.complete(nil)
}

// zoomStepper moves the lens in small steps, for cameras without ZoomInc/ZoomDec.
type zoomStepper struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startStepper starts after prev, the stepper it replaces, has ended.
func (p *PTZ) startStepper(zoom float64, caps PTZCaps, prev *zoomStepper) *zoomStepper {
	ctx, cancel := context.WithCancel(context.Background())
	s := &zoomStepper{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if prev != nil {
			<-prev.done
		}
		p.stepZoom(ctx, zoom, caps)
	}()
	return s
}

// cancelStepperLocked cancels the stepper, including its request in flight,
// without waiting for it.
func (p *PTZ) cancelStepperLocked() *zoomStepper {
	s := p.stepper
	p.stepper = nil
	p.fallback = nil
	if s != nil {
		s.cancel()
	}
	return s
}

func (p *PTZ) stepZoom(ctx context.Context, zoom float64, caps PTZCaps) {
	if ctx.Err() != nil {
		return
	}
	var zf *PtzZoomFocus
	err := p.do(ctx, func(c *Client, cancel <-chan struct{}) (err error) {
		zf, err = c.zoomFocus(cancel)
		return
	})
	if err != nil {
		return
	}

	step := int64(math.Max(1, math.Round(float64(caps.ZoomMax-caps.ZoomMin)/zoomSteps*math.Abs(zoom))))
	if zoom < 0 {
		step = -step
	}
	pos := int64(zf.Zoom.Cur)

	t := time.NewTicker(zoomInterval)
	defer t.Stop()
	for {
		next := min(max(pos+step, int64(caps.ZoomMin)), int64(caps.ZoomMax))
		if next == pos {
			return // at the limit
		}
		if ctx.Err() != nil {
			return
		}
		err := p.do(ctx, func(c *Client, cancel <-chan struct{}) error { return c.zoomTo(uint32(next), cancel) })
		if err != nil {
			return
		}
		pos = next

		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

// unit makes a usable velocity of x: NaN and infinities are 0, the rest is
// held to -1..1.
func unit(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return math.Max(-1, math.Min(1, x))
}

// direction is the PtzControl command and speed of a pan/tilt velocity.
func direction(pan, tilt float64) (Direction, float32) {
	h, v := 0, 0
	if math.Abs(pan) >= ptzDeadZone {
		h = 1
		if pan < 0 {
			h = -1
		}
	}
	if math.Abs(tilt) >= ptzDeadZone {
		v = 1
		if tilt < 0 {
			v = -1
		}
	}

	var dir Direction
	switch {
	case h == 0 && v == 0:
		return DirStop, 0
	case v == 0 && h > 0:
		dir = DirRight
	case v == 0:
		dir = DirLeft
	case h == 0 && v > 0:
		dir = DirUp
	case h == 0:
		dir = DirDown
	case h > 0 && v > 0:
		dir = DirRightUp
	case h > 0:
		dir = DirRightDown
	case v > 0:
		dir = DirLeftUp
	default:
		dir = DirLeftDown
	}

	m := math.Max(math.Abs(pan)*float64(h*h), math.Abs(tilt)*float64(v*v))
	speed := math.Min(ptzMaxSpeed, math.Max(1, math.Round(m*ptzMaxSpeed)))
	return dir, float32(speed)
}

// zoomDirection is the PtzControl command and speed of a zoom velocity.
func zoomDirection(zoom float64) (Direction, float32) {
	speed := math.Min(ptzMaxSpeed, math.Max(1, math.Round(math.Abs(zoom)*ptzMaxSpeed)))
	if zoom < 0 {
		return DirZoomDec, float32(speed)
	}
	return DirZoomInc, float32(speed)
}

// Presets lists the presets stored in the camera.
func (p *PTZ) Presets() (list []Preset, err error) {
	err = p.do(context.Background(), func(c *Client, _ <-chan struct{}) (err error) {
		list, err = c.PTZPresets()
		return
	})
	return
}

// GotoPreset moves the camera to a stored preset. It is a command of the
// loop like Move and Stop: it replaces a command not yet sent and the zoom, and
// once it is acknowledged no Stop owed for the move before it is sent. If it
// fails that Stop is still owed.
func (p *PTZ) GotoPreset(id uint8) error {
	cmd := newPanCmd(DirStop, 0)
	cmd.isPreset, cmd.preset = true, id

	p.moveMu.Lock()
	p.seq++ // gen stays: the move before keeps its safety deadline until the preset is acknowledged
	p.cancelStepperLocked()
	p.setDesiredLocked(cmd)
	p.moveMu.Unlock()

	t := time.NewTimer(moveWait)
	defer t.Stop()
	select {
	case err := <-cmd.done:
		return err
	case <-t.C:
		return errors.New("bc: ptz preset: no result in time")
	}
}

// SetPreset stores the current position, in the lowest free slot if id < 0.
func (p *PTZ) SetPreset(id int, name string) (uint8, error) {
	p.presetMu.Lock()
	defer p.presetMu.Unlock()

	if id < 0 {
		list, err := p.Presets()
		if err != nil {
			return 0, err
		}
		used := map[uint8]bool{}
		for _, it := range list {
			used[it.ID] = true
		}
		id = -1
		for i := 0; i <= ptzMaxPreset; i++ {
			if !used[uint8(i)] {
				id = i
				break
			}
		}
		if id < 0 {
			return 0, errors.New("bc: no free preset slot")
		}
	} else if id > 255 {
		return 0, fmt.Errorf("bc: preset id %d out of range", id)
	}

	err := p.do(context.Background(), func(c *Client, _ <-chan struct{}) error { return c.PTZSetPreset(uint8(id), name) })
	return uint8(id), err
}
