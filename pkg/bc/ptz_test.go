package bc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ptzFor is a controller of a fake camera that is not shared with other tests.
func ptzFor(t *testing.T, f *fakeCamera) *PTZ {
	url := fmt.Sprintf("bc://%s:%s@%s?channel=0", fakeUser, fakePass, f.addr())
	p := newPTZ("test/"+t.Name(), url)
	t.Cleanup(func() { endPTZ(p) })
	return p
}

// endPTZ ends what a test left running in the controller: the stop retries,
// the deadline, the stepper and the control session. The command loop is gone
// when it returns, so it cannot read the variables a later test changes.
func endPTZ(p *PTZ) {
	p.moveMu.Lock()
	p.mayMove, p.stopWanted = false, false
	p.cancelTimerLocked()
	s := p.cancelStepperLocked()
	p.gen++
	close(p.kick)
	p.kick = make(chan struct{})
	p.moveMu.Unlock()
	if s != nil {
		<-s.done
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.moveMu.Lock()
		idle := !p.looping
		p.moveMu.Unlock()
		if idle {
			break
		}
		time.Sleep(time.Millisecond)
	}

	p.mu.Lock()
	if p.idle != nil {
		p.idle.Stop()
	}
	if p.ctrl != nil {
		p.dropLocked(p.ctrl)
	}
	p.mu.Unlock()
}

func TestPTZVelocityMapping(t *testing.T) {
	nan := math.NaN()
	for _, tc := range []struct {
		pan, tilt float64
		want      string
	}{
		{1, 0, "ptz:right:64"},
		{-0.5, 0.5, "ptz:leftUp:32"},
		{0, -0.02, "ptz:stop:0"},
		{nan, 2, "ptz:up:64"},
		{0.001, 0.001, "ptz:stop:0"},
		{-1, -1, "ptz:leftDown:64"},
		{0.3, -0.9, "ptz:rightDown:58"},
		{math.Inf(1), nan, "ptz:stop:0"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			f := newFakeCamera(t)
			p := ptzFor(t, f)
			require.NoError(t, p.Move(Velocity{Pan: tc.pan, Tilt: tc.tilt, HasPanTilt: true}, time.Minute))
			require.NoError(t, p.Stop(true, true))
			require.Equal(t, []string{tc.want, "ptz:stop:0"}, f.events())
		})
	}
}

func TestPTZZoomOnlyDoesNotMovePanTilt(t *testing.T) {
	f := newFakeCamera(t)
	p := ptzFor(t, f)
	require.NoError(t, p.Move(Velocity{Zoom: 1, HasZoom: true}, time.Second))
	require.NoError(t, p.Stop(false, true))
	for _, e := range f.events() {
		require.True(t, strings.HasPrefix(e, "zoom:"), e)
	}
}

func TestPTZSafetyStop(t *testing.T) {
	f := newFakeCamera(t)
	p := ptzFor(t, f)

	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 300*time.Millisecond))
	require.Equal(t, []string{"ptz:right:64"}, f.events())
	require.Eventually(t, func() bool {
		ev := f.events()
		return len(ev) == 2 && ev[1] == "ptz:stop:0"
	}, time.Second, 10*time.Millisecond)
}

func TestPTZSafetyStopDefaultTimeout(t *testing.T) {
	setVar(t, &maxMove, 400*time.Millisecond)
	f := newFakeCamera(t)
	p := ptzFor(t, f)

	start := time.Now()
	// 0 and anything above the maximum both mean maxMove
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 0))
	require.Eventually(t, func() bool { return len(f.events()) == 2 }, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(start), 350*time.Millisecond)
	require.Equal(t, "ptz:stop:0", f.events()[1])

	require.NoError(t, p.Move(Velocity{Pan: -1, HasPanTilt: true}, time.Hour))
	require.Eventually(t, func() bool { return len(f.events()) == 4 }, time.Second, 10*time.Millisecond)
	require.Equal(t, "ptz:stop:0", f.events()[3])
}

func TestPTZStopCancelsSafetyStop(t *testing.T) {
	f := newFakeCamera(t)
	p := ptzFor(t, f)

	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 200*time.Millisecond))
	require.NoError(t, p.Stop(true, true))
	time.Sleep(400 * time.Millisecond)
	require.Equal(t, []string{"ptz:right:64", "ptz:stop:0"}, f.events())
}

func TestPTZNewMoveReplacesTheOld(t *testing.T) {
	f := newFakeCamera(t)
	p := ptzFor(t, f)

	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 200*time.Millisecond))
	require.NoError(t, p.Move(Velocity{Tilt: 1, HasPanTilt: true}, time.Second))
	time.Sleep(400 * time.Millisecond) // the first timer must not stop the second move
	require.Equal(t, []string{"ptz:right:64", "ptz:up:64"}, f.events())

	// zoom only after a pan: the pan is stopped, nothing would stop it later
	require.NoError(t, p.Move(Velocity{Zoom: 1, HasZoom: true}, time.Second))
	require.NoError(t, p.Stop(false, true))
	require.Contains(t, f.events(), "ptz:stop:0")
}

func TestPTZReusesProducerClient(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)

	const host = "reuse-test.invalid"
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	prod, err := NewProducer(conn, fastConfig(), StreamMain,
		"bc://"+fakeUser+":"+fakePass+"@"+host+":9000?channel=0&stream=main")
	require.NoError(t, err)
	t.Cleanup(func() { _ = prod.Stop() })

	// the host does not resolve: a control session could not be opened
	p, err := PTZFor("bc://" + fakeUser + ":" + fakePass + "@" + host + ":9000?channel=0&stream=sub")
	require.NoError(t, err)
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.NoError(t, p.Stop(true, true))
	require.Equal(t, []string{"ptz:right:64", "ptz:stop:0"}, f.sessionEvents())
	require.Equal(t, 1, f.loginCount())
}

func TestPTZControlSessionSingleFlight(t *testing.T) {
	setVar(t, &controlIdle, 200*time.Millisecond)
	f := newMultiFake(t)
	p := ptzFor(t, f)

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Presets()
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, f.count(&f.accepts))
	require.Equal(t, 1, f.loginCount())

	require.Eventually(t, func() bool { return f.count(&f.closed) == 1 }, 2*time.Second, 10*time.Millisecond)

	_, err := p.Presets()
	require.NoError(t, err)
	require.Equal(t, 2, f.count(&f.accepts))
	require.Equal(t, 2, f.loginCount())
}

func TestPTZControlSessionReopensAfterDeath(t *testing.T) {
	f := newMultiFake(t)
	p := ptzFor(t, f)

	_, err := p.Presets()
	require.NoError(t, err)

	p.mu.Lock()
	_ = p.ctrl.conn.Close() // the connection drops
	p.mu.Unlock()

	_, err = p.Presets()
	require.NoError(t, err)
	require.Equal(t, 2, f.loginCount())
}

func TestPTZZoomStepper(t *testing.T) {
	f := newFakeCamera(t)
	p := ptzFor(t, f)

	require.NoError(t, p.Move(Velocity{Zoom: 1, HasZoom: true}, 2*time.Second))
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, p.Stop(false, true))

	zooms := func() (out []uint32) {
		for _, e := range f.events() {
			var n uint32
			_, err := fmt.Sscanf(e, "zoom:%d", &n)
			require.NoError(t, err, e)
			out = append(out, n)
		}
		return
	}
	got := zooms()
	require.GreaterOrEqual(t, len(got), 2)
	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i], got[i-1])
	}
	require.LessOrEqual(t, got[len(got)-1], uint32(33))

	time.Sleep(500 * time.Millisecond)
	require.Equal(t, got, zooms(), "zoom events after Stop")
}

func TestPTZZoomStepperStopsAtLimitAndOut(t *testing.T) {
	setVar(t, &zoomInterval, 10*time.Millisecond)
	f := newFakeCamera(t)
	f.zoomCur = 30
	p := ptzFor(t, f)

	require.NoError(t, p.Move(Velocity{Zoom: 1, HasZoom: true}, 5*time.Second))
	require.Eventually(t, func() bool {
		ev := f.events()
		return len(ev) > 0 && ev[len(ev)-1] == "zoom:33"
	}, time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"zoom:32", "zoom:33"}, f.events())

	require.NoError(t, p.Move(Velocity{Zoom: -1, HasZoom: true}, time.Second))
	require.Eventually(t, func() bool {
		ev := f.events()
		return ev[len(ev)-1] == "zoom:0"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestPTZZoomWithoutLens(t *testing.T) {
	f := newFakeCamera(t)
	f.noPTZ = true
	p := ptzFor(t, f)

	err := p.Move(Velocity{Zoom: 1, HasZoom: true}, time.Second)
	require.Error(t, err)
	require.Empty(t, f.events())
}

func TestPTZSetPresetFreeSlot(t *testing.T) {
	f := newFakeCamera(t)
	f.presets = []Preset{{ID: 0}, {ID: 1}, {ID: 3}}
	p := ptzFor(t, f)

	id, err := p.SetPreset(-1, "x")
	require.NoError(t, err)
	require.Equal(t, uint8(2), id)
	require.Contains(t, f.events(), "preset:setPos:2")

	id, err = p.SetPreset(7, "seven")
	require.NoError(t, err)
	require.Equal(t, uint8(7), id)
}

func TestPTZSetPresetNoFreeSlot(t *testing.T) {
	f := newFakeCamera(t)
	for i := 0; i <= 63; i++ {
		f.presets = append(f.presets, Preset{ID: uint8(i)})
	}
	p := ptzFor(t, f)

	_, err := p.SetPreset(-1, "x")
	require.EqualError(t, err, "bc: no free preset slot")
	require.Empty(t, f.events())
}

func TestPTZCapsCached(t *testing.T) {
	f := newMultiFake(t)
	p := ptzFor(t, f)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			caps, err := p.Caps(t.Context())
			require.NoError(t, err)
			require.Equal(t, PTZCaps{PanTilt: true, Zoom: true, ZoomMax: 33}, caps)
		}()
	}
	wg.Wait()
	require.Equal(t, 1, f.loginCount())

	caps, err := p.Caps(t.Context())
	require.NoError(t, err)
	require.True(t, caps.PanTilt)
}

func TestCameraKey(t *testing.T) {
	for url, want := range map[string]string{
		"bc://u:p@192.168.1.50?stream=main":               "192.168.1.50/0",
		"bc://u:p@192.168.1.50:9000?channel=2&stream=sub": "192.168.1.50/2",
		"bc://u:p@95270000ABCDEFGH?stream=sub":            "95270000abcdefgh/0",
	} {
		got, err := CameraKey(url)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := CameraKey("bc://")
	require.Error(t, err)
}

// wire is a controller on a client over an in-memory connection to a hand
// written camera: respond gets every request and returns the reply and how
// long it is held back, a nil reply is a lost one.
func wire(t *testing.T, respond func(*Message) (*Message, time.Duration)) (*PTZ, <-chan *Message) {
	t.Helper()
	clientConn, camConn := net.Pipe()
	c := NewClient(clientConn, Config{ReplyTimeout: 300 * time.Millisecond, WriteTimeout: time.Second})
	p := newPTZ("wire/"+t.Name(), "bc://unused.invalid")
	p.ctrl = c
	seen := make(chan *Message, 100)
	var writes sync.Mutex
	go func() {
		codec := NewCodec("")
		reader := NewReader(camConn, codec)
		for {
			m, err := reader.ReadMessage()
			if err != nil {
				return
			}
			seen <- m
			r, delay := respond(m)
			if r == nil {
				continue
			}
			b, err := codec.Encode(r)
			if err != nil {
				return
			}
			go func() {
				time.Sleep(delay)
				writes.Lock()
				defer writes.Unlock()
				_, _ = camConn.Write(b)
			}()
		}
	}()
	t.Cleanup(func() {
		endPTZ(p)
		_ = c.Close()
		_ = camConn.Close()
	})
	return p, seen
}

func next(t *testing.T, seen <-chan *Message) *Message {
	t.Helper()
	select {
	case m := <-seen:
		return m
	case <-time.After(time.Second):
		t.Fatal("no wire request")
		return nil
	}
}

func isStop(m *Message) bool {
	return m.MsgID == MsgPTZControl && m.Body.PtzControl.Command == "stop"
}

func isMove(m *Message) bool {
	return m.MsgID == MsgPTZControl && m.Body.PtzControl.Command != "stop"
}

func ok(m *Message) (*Message, time.Duration) { return reply(m, 200), 0 }

// silent drops the replies of the requests that match.
func silent(match func(*Message) bool, n *atomic.Int32) func(*Message) (*Message, time.Duration) {
	return func(m *Message) (*Message, time.Duration) {
		if match(m) && (n == nil || n.Add(-1) >= 0) {
			return nil, 0
		}
		return ok(m)
	}
}

func countCounter(n int32) *atomic.Int32 {
	var c atomic.Int32
	c.Store(n)
	return &c
}

func TestPTZLostMoveACKStillStops(t *testing.T) {
	p, seen := wire(t, silent(isMove, nil))
	err := p.Move(Velocity{Pan: 1, HasPanTilt: true}, 60*time.Millisecond)
	require.Error(t, err)
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)
	select {
	case m := <-seen:
		require.Equal(t, "stop", m.Body.PtzControl.Command)
	case <-time.After(450 * time.Millisecond):
		t.Fatal("Move acknowledgement lost: no safety Stop")
	}
}

func TestPTZLostReplacementACKStillStops(t *testing.T) {
	var moves atomic.Int32
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		if isMove(m) && moves.Add(1) == 2 {
			return nil, 0 // the acknowledgement of the second Move is lost
		}
		return ok(m)
	})
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)
	require.Error(t, p.Move(Velocity{Pan: -1, HasPanTilt: true}, 60*time.Millisecond))
	require.Equal(t, "left", next(t, seen).Body.PtzControl.Command)
	select {
	case m := <-seen:
		require.Equal(t, "stop", m.Body.PtzControl.Command)
	case <-time.After(450 * time.Millisecond):
		t.Fatal("replacement Move acknowledgement lost: no safety Stop")
	}
}

func TestPTZLostStopIsRetried(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{20 * time.Millisecond})
	p, seen := wire(t, silent(isStop, countCounter(1)))
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 500*time.Millisecond))
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)

	require.Error(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seen)))
	require.True(t, isStop(next(t, seen)), "the unacknowledged Stop was not repeated")

	// acknowledged now: nothing more, not even at the old deadline
	time.Sleep(600 * time.Millisecond)
	require.Empty(t, seen)
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	require.False(t, p.mayMove)
	require.False(t, p.stopWanted)
}

func TestPTZLostWatchdogStopIsRetried(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{20 * time.Millisecond})
	p, seen := wire(t, silent(isStop, countCounter(1)))
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 50*time.Millisecond))
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)
	require.True(t, isStop(next(t, seen)))
	require.True(t, isStop(next(t, seen)), "the watchdog Stop was not repeated")
}

func TestPTZStopGivesUp(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{10 * time.Millisecond})
	setVar(t, &stopGiveUp, 150*time.Millisecond)
	p, seen := wire(t, silent(isStop, nil))
	require.Error(t, p.Stop(true, true))
	require.Eventually(t, func() bool {
		p.moveMu.Lock()
		defer p.moveMu.Unlock()
		return !p.mayMove && !p.stopWanted
	}, 2*time.Second, 10*time.Millisecond)
	for len(seen) > 0 {
		<-seen
	}
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, seen, "Stop attempts after giving up")
}

func TestPTZStopNotBlockedByMoveACK(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		if isMove(m) {
			return reply(m, 200), 250 * time.Millisecond
		}
		return ok(m)
	})
	moved := make(chan error, 1)
	go func() { moved <- p.Move(Velocity{Pan: 1, HasPanTilt: true}, 50*time.Millisecond) }()
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(true, true) }()

	select {
	case m := <-seen:
		require.True(t, isStop(m))
	case <-time.After(130 * time.Millisecond):
		t.Fatal("Stop blocked by the delayed Move acknowledgement")
	}
	require.NoError(t, <-moved)
	require.NoError(t, <-stopped)
}

func TestPTZDeadlineCountsFromAcceptance(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		if isMove(m) {
			return reply(m, 200), 250 * time.Millisecond
		}
		return ok(m)
	})
	moved := make(chan error, 1)
	go func() { moved <- p.Move(Velocity{Pan: 1, HasPanTilt: true}, 50*time.Millisecond) }()
	require.Equal(t, "right", next(t, seen).Body.PtzControl.Command)
	select {
	case m := <-seen:
		require.True(t, isStop(m))
	case <-time.After(130 * time.Millisecond):
		t.Fatal("the deadline was not running while the Move acknowledgement was awaited")
	}
	require.NoError(t, <-moved)
}

func TestPTZStopSupersedesUnsentMove(t *testing.T) {
	// the loop is busy with a Stop whose reply is slow: the Move wanted next is
	// replaced by the Stop after it before anything is sent
	release := make(chan struct{})
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		return ok(m)
	})
	p.open = func() (*Client, error) { <-release; return nil, errors.New("closed") }
	p.moveMu.Lock()
	c := p.ctrl
	p.moveMu.Unlock()
	p.mu.Lock()
	p.ctrl = nil // no session: the loop waits for one
	p.mu.Unlock()
	_ = c

	moved := make(chan error, 1)
	go func() { moved <- p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute) }()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.dialer != nil
	}, time.Second, time.Millisecond)

	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(true, true) }()
	require.NoError(t, <-moved) // replaced, never sent
	close(release)
	<-stopped
	for len(seen) > 0 {
		require.False(t, isMove(<-seen), "a Move was sent after the Stop")
	}
}

func TestPTZStopWhileControlSessionOpens(t *testing.T) {
	f := newMultiFake(t)
	p := ptzFor(t, f)
	gate := make(chan struct{})
	p.open = func() (*Client, error) { <-gate; return p.login() }

	moved := make(chan error, 1)
	go func() { moved <- p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute) }()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.dialer != nil
	}, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond) // the loop waits for the session

	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(true, true) }()
	require.NoError(t, <-moved) // replaced before it was sent
	select {
	case <-stopped:
		t.Fatal("Stop returned before there was a session")
	case <-time.After(30 * time.Millisecond):
	}

	close(gate) // the session comes up
	require.NoError(t, <-stopped)
	require.Equal(t, []string{"ptz:stop:0"}, f.events())
	require.Equal(t, 1, f.loginCount())
}

func TestPTZStopReturnsWhileSessionOpening(t *testing.T) {
	setVar(t, &stopWait, 50*time.Millisecond)
	f := newMultiFake(t)
	p := ptzFor(t, f)
	gate := make(chan struct{})
	p.open = func() (*Client, error) { <-gate; return p.login() }

	start := time.Now()
	require.Error(t, p.Stop(true, true))
	require.Less(t, time.Since(start), time.Second)

	close(gate) // the obligation goes on
	require.Eventually(t, func() bool { return len(f.events()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"ptz:stop:0"}, f.events())
}

func TestPTZAcquireUsesProducerRegisteredDuringControlOpen(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint("failed=", failed), func(t *testing.T) {
			p, _ := wire(t, ok)
			q, _ := wire(t, ok)
			p.mu.Lock()
			c := p.ctrl
			p.ctrl = nil
			fl := &dialFlight{done: make(chan struct{})}
			p.dialer = fl
			p.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan *Client, 1)
			go func() {
				got, control, err := p.acquire(ctx)
				if err != nil || control {
					got = nil
				}
				result <- got
			}()
			time.Sleep(30 * time.Millisecond)
			registerClient(p.key, q.ctrl) // the stream is up

			if failed {
				fl.err = errors.New("simulated discovery error")
			} else {
				p.mu.Lock()
				p.ctrl = c
				p.mu.Unlock()
			}
			p.mu.Lock()
			p.dialer = nil
			p.mu.Unlock()
			close(fl.done)
			require.Same(t, q.ctrl, <-result)
		})
	}
}

func TestPTZSupportRequestIsHeaderOnly(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		r := reply(m, 200)
		if m.MsgID == MsgGetSupport {
			n := uint32(1)
			r.Body = &Body{Support: &Support{Items: []SupportItem{{ChnID: 0, PtzType: &n}}}}
		}
		if m.MsgID == MsgGetZoomFocus {
			r.ResponseCode = 400
		}
		return r, 0
	})
	_, err := p.Caps(context.Background())
	require.NoError(t, err)
	m := next(t, seen)
	require.EqualValues(t, 199, m.MsgID)
	require.Nil(t, m.Extension)
	require.Nil(t, m.Body)
}

func TestPTZNonzeroChannelWireMessages(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		r := reply(m, 200)
		switch m.MsgID {
		case MsgGetPTZPreset:
			r.Body = &Body{PtzPreset: &PtzPreset{ChannelID: 3}}
		case MsgGetZoomFocus:
			r.Body = &Body{PtzZoomFocus: &PtzZoomFocus{ChannelID: 3, Zoom: ZoomPos{Max: 33}}}
		}
		return r, 0
	})
	c := p.ctrl
	c.cfg.Channel = 3 // before the first request
	require.NoError(t, c.PTZMove(DirLeftUp, 32))
	require.NoError(t, c.PTZSetPreset(3, "door & porch"))
	require.NoError(t, c.PTZGotoPreset(3))
	_, err := c.PTZPresets()
	require.NoError(t, err)
	_, err = c.ZoomFocus()
	require.NoError(t, err)
	require.NoError(t, c.ZoomTo(20))
	for _, id := range []uint32{18, 19, 19, 190, 294, 295} {
		m := next(t, seen)
		require.Equal(t, id, m.MsgID)
		require.EqualValues(t, 0x6414, m.Class)
		require.EqualValues(t, 3, m.ChannelID)
		require.NotNil(t, m.Extension)
		require.EqualValues(t, 3, *m.Extension.ChannelID)
		switch {
		case m.Body != nil && m.Body.PtzControl != nil:
			require.EqualValues(t, 3, m.Body.PtzControl.ChannelID)
		case m.Body != nil && m.Body.PtzPreset != nil:
			require.EqualValues(t, 3, m.Body.PtzPreset.ChannelID)
		case m.Body != nil && m.Body.StartZoomFocus != nil:
			require.EqualValues(t, 3, m.Body.StartZoomFocus.ChannelID)
		}
	}
}

func TestPTZStopCancelsZoomRequestAndDoesNotWaitForIt(t *testing.T) {
	// the lens requests are not answered at all: Stop must neither wait for a
	// reply timeout (2 s in fastConfig) nor for the stepper's request
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		switch m.MsgID {
		case MsgGetZoomFocus:
			r := reply(m, 200)
			r.Body = &Body{PtzZoomFocus: &PtzZoomFocus{Zoom: ZoomPos{Max: 33}}}
			return r, 0
		case MsgSetZoomFocus:
			return nil, 0
		}
		return ok(m)
	})
	p.ctrl.cfg.ReplyTimeout = 5 * time.Second
	p.capsMu.Lock()
	p.caps = &PTZCaps{PanTilt: true, Zoom: true, ZoomMax: 33}
	p.capsMu.Unlock()

	require.NoError(t, p.Move(Velocity{Pan: 1, Zoom: 1, HasPanTilt: true, HasZoom: true}, time.Minute))
	for { // until the stepper's request is out
		if m := next(t, seen); m.MsgID == MsgSetZoomFocus {
			break
		}
	}
	start := time.Now()
	require.NoError(t, p.Stop(true, true))
	require.Less(t, time.Since(start), time.Second)
}

func TestPTZStopDuringCapabilityValidation(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		r := reply(m, 200)
		switch m.MsgID {
		case MsgGetSupport:
			n := uint32(1)
			r.Body = &Body{Support: &Support{Items: []SupportItem{{ChnID: 0, PtzType: &n}}}}
			return r, 200 * time.Millisecond
		case MsgGetZoomFocus:
			r.Body = &Body{PtzZoomFocus: &PtzZoomFocus{Zoom: ZoomPos{Max: 33}}}
		}
		return r, 0
	})
	moved := make(chan error, 1)
	go func() {
		moved <- p.Move(Velocity{Pan: 1, Zoom: 1, HasPanTilt: true, HasZoom: true}, time.Second)
	}()
	require.EqualValues(t, MsgGetSupport, next(t, seen).MsgID)
	require.NoError(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seen)))
	require.NoError(t, <-moved) // superseded while validating
	observe := time.NewTimer(100 * time.Millisecond)
	defer observe.Stop()
	for {
		select {
		case m := <-seen:
			require.False(t, isMove(m), "the older Move sent pan after the Stop")
			require.NotEqual(t, uint32(MsgSetZoomFocus), m.MsgID, "the older Move started a zoom after the Stop")
		case <-observe.C:
			p.moveMu.Lock()
			defer p.moveMu.Unlock()
			require.Nil(t, p.stepper)
			require.Nil(t, p.timer, "the older Move armed a deadline")
			return
		}
	}
}

func TestPTZMoveSupersededByNewerMoveWhileValidating(t *testing.T) {
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		r := reply(m, 200)
		if m.MsgID == MsgGetSupport {
			n := uint32(1)
			r.Body = &Body{Support: &Support{Items: []SupportItem{{ChnID: 0, PtzType: &n}}}}
			return r, 150 * time.Millisecond
		}
		if m.MsgID == MsgGetZoomFocus {
			r.Body = &Body{PtzZoomFocus: &PtzZoomFocus{Zoom: ZoomPos{Max: 33}}}
		}
		return r, 0
	})
	old := make(chan error, 1)
	go func() { old <- p.Move(Velocity{Pan: 1, Zoom: 1, HasPanTilt: true, HasZoom: true}, time.Minute) }()
	require.EqualValues(t, MsgGetSupport, next(t, seen).MsgID)
	require.NoError(t, p.Move(Velocity{Pan: -1, HasPanTilt: true}, time.Minute)) // no validation needed
	require.Equal(t, "left", next(t, seen).Body.PtzControl.Command)
	require.NoError(t, <-old) // superseded, changes nothing

	time.Sleep(100 * time.Millisecond)
	for len(seen) > 0 {
		m := <-seen
		require.False(t, isMove(m) && m.Body.PtzControl.Command == "right", "the older Move overwrote the newer one")
	}
}

// A Move whose validation fails changes nothing: the move before it keeps its deadline.
func TestPTZFailedValidationKeepsPreviousMove(t *testing.T) {
	f := newFakeCamera(t)
	f.noPTZ = true // no zoom
	p := ptzFor(t, f)
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, 150*time.Millisecond))
	require.Error(t, p.Move(Velocity{Pan: -1, Zoom: 1, HasPanTilt: true, HasZoom: true}, time.Minute))
	require.Eventually(t, func() bool {
		ev := f.events()
		return len(ev) == 2 && ev[0] == "ptz:right:64" && ev[1] == "ptz:stop:0"
	}, time.Second, 10*time.Millisecond)
}

// A late write of an older session must not undo a Stop: all motion commands
// stay on the session of the first, in order. The Stop waits behind the slow
// write (its return is bounded), the obligation is not retired meanwhile.
func TestPTZSwitchSessionsKeepsMotionOnOneSession(t *testing.T) {
	setVar(t, &stopWait, 100*time.Millisecond)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	defer open()
	a, b := net.Pipe()
	gated := &lateGateConn{Conn: a, gate: release, entered: make(chan struct{})}
	c := NewClient(gated, Config{ReplyTimeout: time.Second, WriteTimeout: time.Second})
	p := newPTZ("late-session/"+t.Name(), "bc://unused.invalid")
	p.ctrl = c
	applied := make(chan *Message, 10)
	go func() {
		codec := NewCodec("")
		reader := NewReader(b, codec)
		for {
			m, err := reader.ReadMessage()
			if err != nil {
				return
			}
			applied <- m
			raw, err := codec.Encode(reply(m, 200))
			if err != nil {
				return
			}
			if _, err = b.Write(raw); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { open(); endPTZ(p); _ = c.Close(); _ = b.Close() })
	q, seen := wire(t, ok)

	moved := make(chan error, 1)
	go func() { moved <- p.Move(Velocity{Pan: 1, HasPanTilt: true}, 5*time.Second) }()
	select {
	case <-gated.entered:
	case <-time.After(time.Second):
		t.Fatal("no queued Move")
	}
	registerClient(p.key, q.ctrl) // a stream of the camera starts meanwhile

	require.Error(t, p.Stop(true, true)) // behind the slow write: bounded, not acknowledged yet
	p.moveMu.Lock()
	require.True(t, p.mayMove && p.stopWanted && p.timer != nil, "protection retired before the Stop was acknowledged")
	p.moveMu.Unlock()

	open()
	require.True(t, isMove(next(t, applied)))
	require.True(t, isStop(next(t, applied)), "the Stop must follow the Move on the same session")
	require.NoError(t, <-moved)
	require.Eventually(t, func() bool {
		p.moveMu.Lock()
		defer p.moveMu.Unlock()
		return !p.mayMove && !p.stopWanted && p.timer == nil && p.motion == nil
	}, time.Second, 5*time.Millisecond)
	require.Empty(t, seen, "a command went over the other session")
}

type lateGateConn struct {
	net.Conn
	gate    <-chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (c *lateGateConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.entered); <-c.gate })
	return c.Conn.Write(b)
}

func TestPTZMotionSessionDiesFallsBackToAcquire(t *testing.T) {
	p, seen := wire(t, ok)
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.True(t, isMove(next(t, seen)))
	p.moveMu.Lock()
	dead := p.motion
	p.moveMu.Unlock()
	require.NotNil(t, dead)
	_ = dead.Close()

	q, seenQ := wire(t, ok)
	registerClient(p.key, q.ctrl) // the camera is reachable over another client
	require.NoError(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seenQ)))
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	require.Nil(t, p.motion)
	require.False(t, p.mayMove)
}

func TestPTZMotionSessionIsKeptWhileUnsettled(t *testing.T) {
	// a producer registered after the Move does not get the Stop
	p, seen := wire(t, ok)
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.True(t, isMove(next(t, seen)))
	q, seenQ := wire(t, ok)
	registerClient(p.key, q.ctrl)
	require.NoError(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seen)))
	require.Empty(t, seenQ)

	// settled: the next move may use the new client
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.True(t, isMove(next(t, seenQ)))
}

func TestPTZStopGiveUpKeepsZoomDeadline(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{5 * time.Millisecond})
	setVar(t, &stopGiveUp, 50*time.Millisecond)
	setVar(t, &zoomInterval, 5*time.Millisecond)
	var zooms atomic.Int32
	p, _ := wire(t, func(m *Message) (*Message, time.Duration) {
		r := reply(m, 200)
		if isStop(m) {
			r.ResponseCode = 400
		}
		if m.MsgID == MsgGetZoomFocus {
			r.Body = &Body{PtzZoomFocus: &PtzZoomFocus{Zoom: ZoomPos{Max: 10000}}}
		}
		if m.MsgID == MsgSetZoomFocus {
			zooms.Add(1)
		}
		return r, 0
	})
	p.capsMu.Lock()
	p.caps = &PTZCaps{PanTilt: true, Zoom: true, ZoomMax: 10000}
	p.capsMu.Unlock()
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Second))
	require.Error(t, p.Stop(true, false))
	// a zoom-only move keeps the old pan Stop obligation
	_ = p.Move(Velocity{Zoom: 0.01, HasZoom: true}, 100*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	before := zooms.Load()
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, before, zooms.Load(), "zoom still running after its deadline")
}

func TestPTZStopRetryDoesNotInterruptGotoPreset(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{100 * time.Millisecond})
	var stops atomic.Int32
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		if isStop(m) && stops.Add(1) == 1 {
			return reply(m, 400), 0
		}
		return ok(m)
	})
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Second))
	require.True(t, isMove(next(t, seen)))
	require.Error(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seen)))

	require.NoError(t, p.GotoPreset(3))
	require.EqualValues(t, MsgPTZPreset, next(t, seen).MsgID)
	select {
	case m := <-seen:
		t.Fatalf("a request followed the acknowledged preset: %d", m.MsgID)
	case <-time.After(300 * time.Millisecond):
	}
	p.moveMu.Lock()
	defer p.moveMu.Unlock()
	require.False(t, p.mayMove || p.stopWanted || p.timer != nil || p.motion != nil)
}

func TestPTZFailedGotoPresetKeepsStopRetries(t *testing.T) {
	setVar(t, &stopBackoff, []time.Duration{30 * time.Millisecond})
	var stops atomic.Int32
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		switch {
		case isStop(m) && stops.Add(1) <= 2:
			return reply(m, 400), 0 // the first two Stops fail
		case m.MsgID == MsgPTZPreset:
			return reply(m, 400), 0
		}
		return ok(m)
	})
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Second))
	require.True(t, isMove(next(t, seen)))
	require.Error(t, p.Stop(true, true))
	require.True(t, isStop(next(t, seen)))

	err := p.GotoPreset(9)
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")

	// the Stop retries go on until one is acknowledged
	deadline := time.After(2 * time.Second)
	for stops.Load() < 3 {
		select {
		case <-seen:
		case <-deadline:
			t.Fatal("no further Stop after the failed preset")
		}
	}
	require.Eventually(t, func() bool {
		p.moveMu.Lock()
		defer p.moveMu.Unlock()
		return !p.mayMove && !p.stopWanted
	}, time.Second, 5*time.Millisecond)
}

func TestPTZFailedGotoPresetStopsRunningMove(t *testing.T) {
	// the preset replaced a running move and its deadline: the camera is stopped instead
	p, seen := wire(t, func(m *Message) (*Message, time.Duration) {
		if m.MsgID == MsgPTZPreset {
			return reply(m, 400), 0
		}
		return ok(m)
	})
	require.NoError(t, p.Move(Velocity{Pan: 1, HasPanTilt: true}, time.Minute))
	require.True(t, isMove(next(t, seen)))
	require.Error(t, p.GotoPreset(9))
	require.EqualValues(t, MsgPTZPreset, next(t, seen).MsgID)
	require.True(t, isStop(next(t, seen)))
}

func TestPTZGotoPresetStopsZoomAndReplacesUnsentCommand(t *testing.T) {
	f := newFakeCamera(t)
	f.presets = []Preset{{ID: 5, Name: "x"}}
	p := ptzFor(t, f)
	require.NoError(t, p.Move(Velocity{Zoom: 1, HasZoom: true}, time.Minute))
	require.Eventually(t, func() bool { return len(f.events()) > 0 }, time.Second, 5*time.Millisecond)
	require.NoError(t, p.GotoPreset(5))
	time.Sleep(50 * time.Millisecond)
	n := len(f.events())
	require.Equal(t, "preset:toPos:5", f.events()[n-1])
	time.Sleep(400 * time.Millisecond)
	require.Len(t, f.events(), n, "the zoom went on after the preset")

	require.Error(t, p.GotoPreset(77)) // unknown to the camera
}
