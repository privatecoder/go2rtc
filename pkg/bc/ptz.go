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
// them instead of logging in again.
var (
	registryMu sync.Mutex
	registry   = map[string][]*Client{}
)

func registerClient(key string, c *Client) {
	registryMu.Lock()
	registry[key] = append(registry[key], c)
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
type PTZ struct {
	key, rawURL string

	capsMu sync.Mutex
	caps   *PTZCaps
	flight *capsFlight

	mu     sync.Mutex // control session
	ctrl   *Client
	idle   *time.Timer
	used   time.Time
	dialer *dialFlight

	presetMu sync.Mutex

	moveMu   sync.Mutex // the move state below
	panOn    bool
	stepper  *zoomStepper
	deadline *time.Timer
	gen      uint64
}

func newPTZ(key, rawURL string) *PTZ {
	return &PTZ{key: key, rawURL: rawURL}
}

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
			err := p.do(context.Background(), func(c *Client) (err error) {
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

// do runs fn on a client of the camera. A client that died under the call is
// replaced once.
func (p *PTZ) do(ctx context.Context, fn func(*Client) error) error {
	for attempt := 0; ; attempt++ {
		c, control, err := p.acquire(ctx)
		if err != nil {
			return err
		}
		err = fn(c)
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

// acquire is the client of a running stream or else the control session.
func (p *PTZ) acquire(ctx context.Context) (c *Client, control bool, err error) {
	if c = activeClient(p.key); c != nil {
		return c, false, nil
	}

	for {
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
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
		if fl.err != nil {
			return nil, false, fl.err
		}
		// loop: the session may have been replaced by a stream client or died
	}
}

func (p *PTZ) openControl(fl *dialFlight) {
	c, err := p.login()

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
// before it.
func (p *PTZ) Move(v Velocity, timeout time.Duration) error {
	if timeout <= 0 || timeout > maxMove {
		timeout = maxMove
	}
	pan, tilt, zoom := unit(v.Pan), unit(v.Tilt), unit(v.Zoom)

	p.moveMu.Lock()
	defer p.moveMu.Unlock()

	p.gen++
	p.cancelTimerLocked()
	p.stopStepperLocked()

	// before anything is sent, a camera without zoom must not half-do the move
	var caps PTZCaps
	wantZoom := v.HasZoom && zoom != 0
	if wantZoom {
		var err error
		if caps, err = p.Caps(context.Background()); err == nil && !caps.Zoom {
			err = errors.New("bc: camera has no zoom")
		}
		if err != nil {
			if p.panOn { // its timer is gone, do not leave it running
				_ = p.stopPanLocked()
			}
			return err
		}
	}

	dir, speed := DirStop, float32(0)
	sendPT := v.HasPanTilt
	if sendPT {
		dir, speed = direction(pan, tilt)
	}
	// a pan or tilt still running from the move before would lose its timer
	if sendPT || p.panOn {
		if err := p.do(context.Background(), func(c *Client) error { return c.PTZMove(dir, speed) }); err != nil {
			p.panOn = false
			return err
		}
		p.panOn = dir != DirStop
	}

	if wantZoom {
		p.stepper = p.startStepper(zoom, caps)
	}

	if p.panOn || p.stepper != nil {
		gen := p.gen
		p.deadline = time.AfterFunc(timeout, func() { p.expireMove(gen) })
	}
	return nil
}

// expireMove is the safety stop.
func (p *PTZ) expireMove(gen uint64) {
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	if p.gen != gen {
		return
	}
	p.stopStepperLocked()
	_ = p.stopPanLocked()
}

// Stop ends the pan/tilt and/or the zoom of the current move. The camera gets
// a stop for pan/tilt even if no move is known, the move may have been
// started by someone else.
func (p *PTZ) Stop(panTilt, zoom bool) error {
	p.moveMu.Lock()
	defer p.moveMu.Unlock()

	var err error
	if zoom {
		p.stopStepperLocked()
	}
	if panTilt {
		err = p.stopPanLocked()
	}
	if !p.panOn && p.stepper == nil {
		p.gen++
		p.cancelTimerLocked()
	}
	return err
}

func (p *PTZ) cancelTimerLocked() {
	if p.deadline != nil {
		p.deadline.Stop()
		p.deadline = nil
	}
}

func (p *PTZ) stopPanLocked() error {
	p.panOn = false
	return p.do(context.Background(), func(c *Client) error { return c.PTZMove(DirStop, 0) })
}

// zoomStepper moves the lens in small steps, as Baichuan has no continuous zoom.
type zoomStepper struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *PTZ) startStepper(zoom float64, caps PTZCaps) *zoomStepper {
	ctx, cancel := context.WithCancel(context.Background())
	s := &zoomStepper{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		p.stepZoom(ctx, zoom, caps)
	}()
	return s
}

func (p *PTZ) stopStepperLocked() {
	if s := p.stepper; s != nil {
		p.stepper = nil
		s.cancel()
		<-s.done
	}
}

func (p *PTZ) stepZoom(ctx context.Context, zoom float64, caps PTZCaps) {
	var zf *PtzZoomFocus
	err := p.do(ctx, func(c *Client) (err error) {
		zf, err = c.ZoomFocus()
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
		err := p.do(ctx, func(c *Client) error { return c.ZoomTo(uint32(next)) })
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

// Presets lists the presets stored in the camera.
func (p *PTZ) Presets() (list []Preset, err error) {
	err = p.do(context.Background(), func(c *Client) (err error) {
		list, err = c.PTZPresets()
		return
	})
	return
}

func (p *PTZ) GotoPreset(id uint8) error {
	return p.do(context.Background(), func(c *Client) error { return c.PTZGotoPreset(id) })
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

	err := p.do(context.Background(), func(c *Client) error { return c.PTZSetPreset(uint8(id), name) })
	return uint8(id), err
}
