package udp

import (
	"bytes"
	"context"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cooldownFails(uid string) int {
	state.mu.Lock()
	defer state.mu.Unlock()
	if c := state.cooldown[uid]; c != nil {
		return c.fails
	}
	return 0
}

// --- F1: the packet-id boundary

func TestSequenceLimitOnSend(t *testing.T) {
	var infos atomic.Int32
	cfg := fastCfg()
	cfg.SeqLimit = 5
	cfg.Log = Logger{Infof: func(string, ...any) { infos.Add(1) }}
	c, cam := newPair(t, cfg)

	for i := 0; i < 5; i++ {
		_, err := c.Write([]byte("x"))
		require.NoError(t, err)
	}
	_, err := c.Write([]byte("x"))
	require.ErrorIs(t, err, ErrSequenceLimit)

	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrSequenceLimit)
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_DISC") == 1 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return infos.Load() == 1 }, time.Second, time.Millisecond, "logged once, at info")
}

func TestSequenceLimitOnReceive(t *testing.T) {
	cfg := fastCfg()
	cfg.SeqLimit = 5
	c, cam := newPair(t, cfg)

	cam.data(0, []byte("a"))
	cam.data(1, []byte("b"))
	require.Equal(t, "ab", string(readN(t, c, 2)))

	// the camera's ids reached the limit: end before they wrap in the ACK group
	cam.data(5, []byte("far"))
	_, err := c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrSequenceLimit)

	// also when the stream gets there in order
	c2, cam2 := newPair(t, cfg)
	for i := 0; i < 5; i++ {
		cam2.data(uint32(i), []byte("x"))
	}
	require.Equal(t, "xxxxx", string(readN(t, c2, 5)), "what was delivered is still readable")
	_, err = c2.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrSequenceLimit)

	// the default is far from any boundary but below it
	require.Less(t, uint32(seqLimit), uint32(1<<30))
	require.Greater(t, uint32(seqLimit), uint32(1<<29))
}

func TestAckForPacketsNeverSent(t *testing.T) {
	cfg := fastCfg()
	cfg.Timeout = 300 * time.Millisecond
	cfg.ResendInterval = time.Hour
	c, cam := newPair(t, cfg)

	_, err := c.Write(bytes.Repeat([]byte{1}, 3*MaxPayload)) // ids 0..2
	require.NoError(t, err)

	// a delayed ACK of a previous cycle, a forged one, a selective map past the end
	for _, id := range []uint32{3, 100, 0xfffffffe} {
		cam.send(&Ack{ConnID: testCID, PacketID: id})
	}
	cam.send(&Ack{ConnID: testCID, PacketID: 1, Payload: []byte{0, 1, 1}})
	require.Eventually(t, func() bool { return c.Stats().Unacked == 1 }, time.Second, time.Millisecond)
	require.Equal(t, 3, c.Stats().ProtocolErrors)

	// and an ACK for something never sent is no sign of life either
	for i := 0; i < 15; i++ {
		cam.send(&Ack{ConnID: testCID, PacketID: 500})
		time.Sleep(30 * time.Millisecond)
	}
	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrTimeout)
}

func TestAckSentinelPairs(t *testing.T) {
	good, _ := EmptyAck(5).Marshal()
	_, err := Decode(good)
	require.NoError(t, err)

	for name, a := range map[string]*Ack{
		"group without packet": {ConnID: 5, GroupID: NoAck, PacketID: 7},
		"packet without group": {ConnID: 5, GroupID: 0, PacketID: NoAck},
		"second group":         {ConnID: 5, GroupID: 1, PacketID: 7},
	} {
		b, _ := a.Marshal()
		_, err := Decode(b)
		require.Error(t, err, name)
	}
}

// --- F2: only this session counts

func TestOtherSessionIsNotLife(t *testing.T) {
	cfg := fastCfg()
	cfg.Timeout = 250 * time.Millisecond
	c, cam := newPair(t, cfg)

	// traffic from the right endpoint for other ids, as after a camera reboot
	// or from a delayed older session: Data, ACKs and heartbeats
	for i := 0; i < 15; i++ {
		cam.send(&Data{ConnID: testCID + 1, PacketID: 0, Payload: []byte("old")})
		cam.send(&Ack{ConnID: testCID + 1, PacketID: 0})
		cam.send(&Discovery{TID: 4, XML: &D2cHb{CID: testCID + 1, DID: testDID + 1}})
		time.Sleep(25 * time.Millisecond)
	}
	_, err := c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrTimeout, "stale traffic kept the connection alive")
	require.Greater(t, c.Stats().Stale, 20)
}

func TestMismatchedDisconnectsAreIgnored(t *testing.T) {
	cfg := fastCfg()
	cfg.SID = 42
	c, cam := newPair(t, cfg)

	cam.send(&Discovery{TID: 1, XML: &D2cDisc{CID: testCID + 1, DID: testDID}})
	cam.send(&Discovery{TID: 1, XML: &D2cDisc{CID: testCID, DID: testDID + 1}})
	cam.send(&Discovery{TID: 1, XML: &R2cDisc{SID: 41}})
	time.Sleep(50 * time.Millisecond)
	select {
	case <-c.done:
		t.Fatal("closed by a disconnect of another session")
	default:
	}
	require.Equal(t, 3, c.Stats().Stale)

	cam.send(&Discovery{TID: 1, XML: &D2cDisc{CID: testCID, DID: testDID}})
	_, err := c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrPeerClosed)
}

func TestHeartbeatOfAnotherSessionIsNotAnswered(t *testing.T) {
	c, cam := newPair(t, ConnConfig{HBInterval: time.Hour, AckInterval: 5 * time.Millisecond, Timeout: 5 * time.Second})
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_HB") == 1 }, time.Second, time.Millisecond) // the first one

	cam.send(&Discovery{TID: 3, XML: &D2cHb{CID: testCID + 5, DID: testDID + 5}})
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 1, cam.countDiscs("C2D_HB"), "no answer to a heartbeat of another session")
	require.Equal(t, 1, c.Stats().Stale)

	cam.send(&Discovery{TID: 3, XML: &D2cHb{CID: testCID, DID: testDID}})
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_HB") == 2 }, time.Second, time.Millisecond)
}

// --- F3: receive memory

func TestOversizedDataIsDropped(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	cam.data(0, make([]byte, MaxPayload+1))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, c.Stats().ProtocolErrors)
	require.Zero(t, c.Stats().PacketsWant)

	cam.data(0, make([]byte, MaxPayload)) // the limit itself is fine
	require.Len(t, readN(t, c, MaxPayload), MaxPayload)
}

func TestReorderBufferIsBoundedInBytes(t *testing.T) {
	cfg := fastCfg()
	cfg.ReorderLimit = 4000
	cfg.GapSkip = time.Hour
	c, cam := newPair(t, cfg)

	for id := uint32(1); id <= 10; id++ { // 0 is missing
		cam.data(id, bytes.Repeat([]byte{byte(id)}, MaxPayload))
	}
	require.Eventually(t, func() bool { return c.Stats().ReorderDropped >= 7 }, time.Second, time.Millisecond)
	s := c.Stats()
	require.Equal(t, 3, s.Reorder)
	require.LessOrEqual(t, s.ReorderBytes, 4000)

	// the gap closes, what was kept is delivered; the rest comes again from the camera
	cam.data(0, []byte{0})
	got := readN(t, c, 1+3*MaxPayload)
	require.Equal(t, byte(3), got[len(got)-1])
	require.Zero(t, c.Stats().ReorderBytes)
}

func TestEmptyPacketsAdvanceButAreNotQueued(t *testing.T) {
	cfg := fastCfg()
	cfg.RxLimit = 1024
	c, cam := newPair(t, cfg)

	// nobody reads
	for id := uint32(0); id < 2000; id++ {
		cam.data(id, nil)
	}
	require.Eventually(t, func() bool { return c.Stats().PacketsWant == 2000 }, 2*time.Second, time.Millisecond)
	c.mu.Lock()
	entries := len(c.rx)
	c.mu.Unlock()
	require.Zero(t, entries, "empty packets must not pile up in the read queue")
	require.Eventually(t, func() bool { return cam.lastAck().PacketID == 1999 }, time.Second, time.Millisecond, "but they are acknowledged")

	cam.data(2000, []byte("x"))
	require.Equal(t, "x", string(readN(t, c, 1)))
}

// --- F8: diagnostics and resends do not stall the transport

func TestBlockedLoggerStallsNothing(t *testing.T) {
	release := make(chan struct{})
	var logged atomic.Int32
	var conn atomic.Pointer[Conn]
	cfg := fastCfg()
	cfg.GapSkip = 50 * time.Millisecond
	cfg.Timeout = 250 * time.Millisecond
	cfg.Log = Logger{Warnf: func(string, ...any) {
		logged.Add(1)
		if c := conn.Load(); c != nil {
			_ = c.Stats() // a sink that looks at the connection must not deadlock
		}
		<-release
	}}
	c, cam := newPair(t, cfg)
	conn.Store(c)
	defer close(release)

	cam.data(0, []byte("a"))
	cam.data(2, []byte("c")) // 1 never comes: a gap skip, hence a warning
	require.Eventually(t, func() bool { return logged.Load() == 1 }, 2*time.Second, time.Millisecond)

	// the logger is stuck, the transport is not
	start := time.Now()
	_ = c.Stats()
	require.Less(t, time.Since(start), 50*time.Millisecond)
	require.Equal(t, "ac", string(readN(t, c, 2)))

	acks := func() int { cam.mu.Lock(); defer cam.mu.Unlock(); return len(cam.acks) }
	before := acks()
	time.Sleep(100 * time.Millisecond)
	require.Greater(t, acks()-before, 5, "ACKs go on")

	// and the silence timeout still works
	_, err := c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrTimeout)
}

func TestStalledSendsDoNotStopMaintenance(t *testing.T) {
	cfg := fastCfg()
	cfg.ResendInterval = 40 * time.Millisecond
	cfg.Timeout = time.Minute
	c, _ := newPair(t, cfg)

	_, err := c.Write(bytes.Repeat([]byte{1}, 300*MaxPayload)) // 300 packets, no ACK
	require.NoError(t, err)

	// from now on every Data datagram takes 30 ms to send, ACKs and heartbeats are timed
	var mu sync.Mutex
	var ackAt []time.Time
	hook := func(b []byte, _ netip.AddrPort) error {
		switch p, _ := Decode(b); p.(type) {
		case *Data:
			time.Sleep(30 * time.Millisecond)
		case *Ack:
			mu.Lock()
			ackAt = append(ackAt, time.Now())
			mu.Unlock()
		}
		return nil
	}
	c.sock.sendHook.Store(&hook)
	time.Sleep(700 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(ackAt), 10)
	var worst time.Duration
	for i := 1; i < len(ackAt); i++ {
		worst = max(worst, ackAt[i].Sub(ackAt[i-1]))
	}
	// the whole pass would be 9 s of sleeping; one tick sends one stalled packet at most
	require.Less(t, worst, 120*time.Millisecond, "maintenance stopped for %s", worst)
	require.Less(t, c.Stats().Resends, 60)
}

func TestResendsAreBatched(t *testing.T) {
	cfg := fastCfg()
	cfg.ResendInterval = 30 * time.Millisecond
	cfg.ResendBatch = 10
	c, cam := newPair(t, cfg)

	_, err := c.Write(bytes.Repeat([]byte{1}, 100*MaxPayload))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(cam.dataIDs()) >= 100+100 }, 3*time.Second, 5*time.Millisecond, "every packet is sent again")

	// every id came again, in passes
	seen := map[uint32]int{}
	for _, id := range cam.dataIDs() {
		seen[id]++
	}
	for id := uint32(0); id < 100; id++ {
		require.GreaterOrEqual(t, seen[id], 2, "packet %d", id)
	}
}

// --- F4: tids

func TestOffersAreNotHiddenByAnUnrelatedWaiter(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	s, err := newSession(o)
	require.NoError(t, err)
	defer s.close()

	waiter, unsub, err := s.subscribe(200) // some request that happens to use tid 200
	require.NoError(t, err)
	defer unsub()
	offers, unlisten := s.listen()
	defer unlisten()

	peer := netip.MustParseAddrPort("127.0.0.1:9")
	offer, _ := (&Discovery{TID: 200, XML: &D2cT{SID: 1, Conn: "map", CID: 2, DID: 3}}).Marshal()
	s.onPacket(offer, peer)
	require.Len(t, waiter, 1)
	require.Len(t, offers, 1, "the offer must reach the listener")

	// a confirmation likewise; everything else stays with the waiter
	cfm, _ := (&Discovery{TID: 200, XML: &D2cCfm{SID: 1}}).Marshal()
	s.onPacket(cfm, peer)
	require.Len(t, offers, 2)
	hb, _ := (&Discovery{TID: 200, XML: &D2cHb{}}).Marshal()
	s.onPacket(hb, peer)
	require.Len(t, waiter, 3)
	require.Len(t, offers, 2)
}

func TestTIDsAreReservedAtomically(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	s, err := newSession(o)
	require.NoError(t, err)
	defer s.close()

	var mu sync.Mutex
	seen := map[uint32]bool{}
	var unsubs []func()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tid, _, unsub, err := s.subscribeNew()
			mu.Lock()
			defer mu.Unlock()
			if assert.NoError(t, err) {
				assert.False(t, seen[tid], "tid %d handed out twice", tid)
				seen[tid] = true
				unsubs = append(unsubs, unsub)
			}
		}()
	}
	wg.Wait()
	require.Len(t, seen, 200)

	// the space runs out without hanging
	for {
		_, _, unsub, err := s.subscribeNew()
		if err != nil {
			require.ErrorContains(t, err, "no free tid")
			break
		}
		unsubs = append(unsubs, unsub)
	}
	require.LessOrEqual(t, len(unsubs), 255)
	for _, u := range unsubs {
		u()
	}
	_, _, unsub, err := s.subscribeNew()
	require.NoError(t, err)
	unsub()
}

// --- F5: time running out counts

func TestDeadlineCountsForTheCooldown(t *testing.T) {
	c := newCloud(t)
	silent := newEndpoint(t).serve(nil) // a lookup server that never answers
	o := c.opts(t, MethodRemote)
	o.LookupHosts = []string{silent.addr.String()}
	o.ReplyTimeout = 5 * time.Second
	o.BackoffBase = 100 * time.Millisecond
	o.BackoffMax = time.Second
	uid := uniqueUID()

	// bc.Dial runs Discover under a context with a deadline
	for i := 1; i <= 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		_, err := Discover(ctx, uid, o)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, i, cooldownFails(uid), "once per call")
	}
	require.Greater(t, cooldownWait(uid), time.Duration(0))

	// a cancel by the caller is not a failure of the camera
	uid2 := uniqueUID()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, err := Discover(ctx, uid2, o)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, cooldownFails(uid2))
}

func TestCooldownIsCheckedAgainAfterWaiting(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodRemote)
	o.BackoffBase = 200 * time.Millisecond
	o.BackoffMax = 2 * time.Second
	uid := uniqueUID()

	cooldownFail(uid, &o) // until +200 ms
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := Discover(ctx, uid, o)
		if conn != nil {
			conn.Close()
		}
		done <- err
	}()

	// another call for the same camera fails meanwhile and pushes the cooldown out
	time.Sleep(100 * time.Millisecond)
	cooldownFail(uid, &o) // until +100 ms + 400 ms

	require.NoError(t, <-done)
	require.GreaterOrEqual(t, time.Since(start), 450*time.Millisecond, "went ahead on the old, shorter cooldown")
}

// --- F6: a confirmed path does not wait for one that does not answer

func TestConfirmedPathWinsOverASilentRelay(t *testing.T) {
	c := newCloud(t)
	c.noDmap.Store(true) // no punch, straight to the relay, which never answers
	c.relayWorks.Store(false)
	c.lanDelay.Store(int64(650 * time.Millisecond))
	o := c.opts(t, MethodRelay)
	o.ReplyTimeout = 3 * time.Second

	start := time.Now()
	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.lan)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 600*time.Millisecond)
	require.Less(t, elapsed, 1500*time.Millisecond, "waited for the relay's request to run out")

	// with an overall deadline shorter than the relay's wait it still works
	c2 := newCloud(t)
	c2.noDmap.Store(true)
	c2.relayWorks.Store(false)
	c2.lanDelay.Store(int64(400 * time.Millisecond))
	o2 := c2.opts(t, MethodRelay)
	o2.ReplyTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	conn2, err := Discover(ctx, uniqueUID(), o2)
	require.NoError(t, err)
	conn2.Close()
}

// --- F7: refusals

func TestRefusalWithoutAddressesIsPrompt(t *testing.T) {
	c := newCloud(t)
	c.refuseEmpty.Store(true)
	o := c.opts(t, MethodRemote)
	o.ReplyTimeout = 2 * time.Second
	o.setDefaults()
	s, err := newSession(&o)
	require.NoError(t, err)
	defer s.close()

	start := time.Now()
	_, err = s.registerAddress(context.Background(), uniqueUID(), lookupInfo{reg: c.reg.addr, relay: c.relay.addr})
	require.ErrorIs(t, err, ErrRegisterFail)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestNegativeAndWrongConfirmations(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodRemote)
	o.ReplyTimeout = 300 * time.Millisecond
	o.setDefaults()
	s, err := newSession(&o)
	require.NoError(t, err)
	defer s.close()
	reg := &regResult{reg: c.reg.addr, dev: c.lan.addr, dmap: c.pub.addr, relay: c.relay.addr, sid: c.sid}

	// the camera or relay says no
	c.cfmRsp.Store(-3)
	_, err = s.clientInitiatedRelay(context.Background(), reg)
	require.ErrorIs(t, err, ErrConfirmFailed)
	_, err = s.clientInitiatedDev(context.Background(), reg)
	require.ErrorIs(t, err, ErrConfirmFailed)
	require.False(t, s.won.Load(), "a refusal must not win")

	// confirmation for another path is no confirmation
	c.cfmRsp.Store(0)
	c.cfmConn.Store("map")
	_, err = s.clientInitiatedRelay(context.Background(), reg)
	require.ErrorIs(t, err, ErrNoReply)
	_, err = s.clientInitiatedDev(context.Background(), reg)
	require.ErrorIs(t, err, ErrNoReply)
	require.False(t, s.won.Load())

	// the right one wins
	c.cfmConn.Store("")
	f, err := s.clientInitiatedDev(context.Background(), reg)
	require.NoError(t, err)
	require.Equal(t, c.lan.addr, f.peer)
	require.Equal(t, c.sid, f.sid)
}

func TestRefusedPathDoesNotStopTheOthers(t *testing.T) {
	// the LAN path is refused, the relay answers properly
	c := newCloud(t)
	var refuse atomic.Bool
	refuse.Store(true)
	c.lan.mu.Lock()
	inner := c.lan.onMsg
	c.lan.onMsg = func(e *endpoint, d *Discovery, from netip.AddrPort) {
		if m, ok := d.XML.(*C2dT); ok && m.Conn == "local" && refuse.Load() {
			e.reply(from, d.TID, &D2cCfm{SID: c.sid, Conn: "local", Rsp: -3, CID: m.CID, DID: c.did})
			return
		}
		inner(e, d, from)
	}
	c.lan.mu.Unlock()
	c.mapOffers.Store(false)
	o := c.opts(t, MethodRelay)

	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.relay)
}

// --- F9: the cache knows the lookup policy

func TestLookupCacheKeyHasThePolicy(t *testing.T) {
	a, b := Options{Region: "Europe (Germany)"}, Options{Region: " europe (germany) "}
	other := Options{Region: "Middle East"}
	all := Options{}
	for _, o := range []*Options{&a, &b, &other, &all} {
		o.setDefaults()
	}
	require.Equal(t, cacheKey("U", &a), cacheKey("U", &b), "the same policy, spelled differently")
	require.NotEqual(t, cacheKey("U", &a), cacheKey("U", &other))
	require.NotEqual(t, cacheKey("U", &a), cacheKey("U", &all))
	require.NotEqual(t, cacheKey("U", &a), cacheKey("V", &a))
}

func TestLookupCacheNotUsedAcrossPolicies(t *testing.T) {
	cloudA, cloudB := newCloud(t), newCloud(t)
	oA := cloudA.opts(t, MethodRemote)
	oA.NoCache = false
	oB := cloudB.opts(t, MethodRemote)
	oB.NoCache = false
	uid := uniqueUID()

	conn, err := discover(t, oA, uid)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, int32(1), cloudA.lookups.Load())

	// the same camera, another lookup server: the new one is asked
	conn, err = discover(t, oB, uid)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, int32(1), cloudB.lookups.Load())
	require.Equal(t, int32(1), cloudA.lookups.Load())
	require.Equal(t, int32(0), cloudA.registers.Load()-1, "the old server was not contacted again")

	// within one policy the cache works as before
	conn, err = discover(t, oB, uid)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, int32(1), cloudB.lookups.Load())
}
