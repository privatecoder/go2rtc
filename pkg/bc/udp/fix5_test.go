package udp

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- R4: waiting for a cooldown is not a failure

func TestCooldownWaitIsNotAFailure(t *testing.T) {
	c := newCloud(t) // the servers are healthy
	o := c.opts(t, MethodRemote)
	o.BackoffBase = 200 * time.Millisecond
	o.BackoffMax = 5 * time.Second
	o.ReplyTimeout = 200 * time.Millisecond // the LAN query gives up quickly
	uid := uniqueUID()

	for i := 0; i < 3; i++ {
		cooldownFail(uid, &o) // 200, 400, 800 ms: until about +800 ms
	}
	fails := cooldownFails(uid)
	start := time.Now()

	// go2rtc calls again and again with a deadline shorter than the cooldown
	var calls int
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		conn, err := Discover(ctx, uid, o)
		cancel()
		calls++
		if err == nil {
			conn.Close()
			break
		}
		var cd *CooldownError
		require.ErrorAs(t, err, &cd, "call %d: %v", calls, err)
		require.Greater(t, cd.Wait, time.Duration(0))
		require.Equal(t, fails, cooldownFails(uid), "waiting renewed the cooldown (call %d)", calls)
		require.Zero(t, c.lookups.Load(), "the servers were contacted inside the cooldown")
		require.Less(t, time.Since(start), 3*time.Second, "never recovered")
	}

	// recovered within one cooldown period (plus the call that had to end first)
	require.Less(t, time.Since(start), 800*time.Millisecond+600*time.Millisecond)
	require.GreaterOrEqual(t, calls, 2)
	require.Zero(t, cooldownWait(uid))
}

func TestCooldownCap(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	require.Equal(t, 30*time.Second, o.BackoffMax)
	uid := uniqueUID()
	for i := 0; i < 20; i++ {
		cooldownFail(uid, o)
	}
	require.LessOrEqual(t, cooldownWait(uid), 30*time.Second)
	cooldownReset(uid)
}

func TestCooldownErrorMessage(t *testing.T) {
	require.Equal(t, "udp: waiting for the P2P cooldown, retry in 12s", (&CooldownError{Wait: 12 * time.Second}).Error())
	var cd *CooldownError
	require.True(t, errors.As(errors.Join(errors.New("x"), &CooldownError{Wait: time.Second}), &cd))
}

// --- R6: the camera's own tid

func TestAcceptWithACollidingTID(t *testing.T) {
	c := newCloud(t) // the camera offers the map connection under tid 200
	o := c.opts(t, MethodMap)
	o.ReplyTimeout = 2 * time.Second
	o.setDefaults()
	s, err := newSession(&o)
	require.NoError(t, err)
	defer s.close()

	// an unrelated request owns tid 200
	_, unsub, err := s.subscribe(200)
	require.NoError(t, err)
	defer unsub()

	reg := &regResult{reg: c.reg.addr, dev: c.lan.addr, dmap: c.pub.addr, relay: c.relay.addr, sid: c.sid}
	f, err := s.deviceInitiatedMap(context.Background(), reg, 2*time.Second)
	require.NoError(t, err, "a valid offer must not fail on the tid it happens to use")
	require.Equal(t, c.pub.addr, f.peer)
	require.Equal(t, "map", f.path)
	require.Equal(t, c.did, f.did)
}

// --- R9: connections do not leave goroutines behind with a stuck log sink

func TestStuckLogSinkDoesNotPileUp(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	cfg := fastCfg()
	cfg.GapSkip = 20 * time.Millisecond
	cfg.Log = Logger{Warnf: func(string, ...any) { <-release }, Infof: func(string, ...any) { <-release }}

	// let the shared workers exist before counting
	warm, wcam := newPair(t, cfg)
	wcam.data(0, []byte("a"))
	wcam.data(2, []byte("c"))
	time.Sleep(80 * time.Millisecond)
	warm.Close()
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()

	for i := 0; i < 40; i++ {
		c, cam := newPair(t, cfg)
		cam.data(0, []byte("a"))
		cam.data(2, []byte("c")) // a gap skip: a warning for the stuck sink
		time.Sleep(40 * time.Millisecond)
		c.Close()
		cam.pc.Close()
	}
	time.Sleep(200 * time.Millisecond)
	require.LessOrEqual(t, runtime.NumGoroutine(), base+6, "goroutines pile up with every connection")
}
