package bc

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ptzFor is a controller of a fake camera that is not shared with other tests.
func ptzFor(t *testing.T, f *fakeCamera) *PTZ {
	url := fmt.Sprintf("bc://%s:%s@%s?channel=0", fakeUser, fakePass, f.addr())
	p := newPTZ("test/"+t.Name(), url)
	t.Cleanup(func() {
		p.mu.Lock()
		if p.ctrl != nil {
			p.dropLocked(p.ctrl)
		}
		p.mu.Unlock()
	})
	return p
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
