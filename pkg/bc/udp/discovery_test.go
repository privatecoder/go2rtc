package udp

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// endpoint is a UDP server of the fake cloud.
type endpoint struct {
	t      *testing.T
	pc     *net.UDPConn
	addr   netip.AddrPort
	onMsg  func(e *endpoint, d *Discovery, from netip.AddrPort)
	onData func(from netip.AddrPort)

	mu     sync.Mutex
	client netip.AddrPort // last peer that talked to us
	seen   []Message
}

func newEndpoint(t *testing.T) *endpoint {
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	e := &endpoint{t: t, pc: pc, addr: pc.LocalAddr().(*net.UDPAddr).AddrPort()}
	t.Cleanup(func() { pc.Close() })
	return e
}

// serve starts answering; endpoints are created first so handlers can use each other's addresses.
func (e *endpoint) serve(onMsg func(e *endpoint, d *Discovery, from netip.AddrPort)) *endpoint {
	e.onMsg = onMsg
	go e.loop()
	return e
}

func (e *endpoint) setOnData(f func(netip.AddrPort)) {
	e.mu.Lock()
	e.onData = f
	e.mu.Unlock()
}

func (e *endpoint) loop() {
	buf := make([]byte, 65536)
	for {
		n, from, err := e.pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			return
		}
		p, err := Decode(buf[:n])
		if err != nil {
			continue
		}
		e.mu.Lock()
		e.client = from
		e.mu.Unlock()
		if d, ok := p.(*Discovery); ok {
			e.mu.Lock()
			e.seen = append(e.seen, d.XML)
			e.mu.Unlock()
			if e.onMsg != nil {
				e.onMsg(e, d, from)
			}
		} else {
			e.mu.Lock()
			f := e.onData
			e.mu.Unlock()
			if f != nil {
				f(from)
			}
		}
	}
}

func (e *endpoint) send(to netip.AddrPort, p Packet) {
	b, err := p.Marshal()
	require.NoError(e.t, err)
	_, _ = e.pc.WriteToUDPAddrPort(b, to)
}

func (e *endpoint) reply(to netip.AddrPort, tid uint32, m Message) {
	e.send(to, &Discovery{TID: tid, XML: m})
}

func (e *endpoint) count(name string, match func(Message) bool) (n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, m := range e.seen {
		if m.XMLName() == name && (match == nil || match(m)) {
			n++
		}
	}
	return
}

func ipport(a netip.AddrPort) *IpPort { return &IpPort{IP: a.Addr().String(), Port: a.Port()} }

// cloud is the Reolink side and the camera, all on localhost: lookup server,
// register server, relay, and the camera's LAN and public sockets.
type cloud struct {
	lookup, reg, relay, lan, pub *endpoint

	sid uint32
	did int32

	noDmap      atomic.Bool  // the register server knows no public address
	refuseEmpty atomic.Bool  // the register server refuses without any address
	cfmRsp      atomic.Int32 // rsp of every D2C_CFM
	cfmConn     atomic.Value // string: the conn every D2C_CFM claims, instead of the right one
	lanDelay    atomic.Int64 // ns the camera takes to answer on its LAN address
	mapOffers   atomic.Bool  // the camera answers hole punches
	lanAnswers  atomic.Bool  // the camera answers on its LAN address
	relayWorks  atomic.Bool
	regFails    atomic.Int32 // refuse this many registrations first
	lookups     atomic.Int32
	registers   atomic.Int32
}

func newCloud(t *testing.T) *cloud {
	c := &cloud{sid: 62098713, did: 80}
	c.mapOffers.Store(true)
	c.lanAnswers.Store(true)
	c.relayWorks.Store(true)

	cfm := func(conn string, cid int32) *D2cCfm {
		if o, _ := c.cfmConn.Load().(string); o != "" {
			conn = o
		}
		return &D2cCfm{SID: c.sid, Conn: conn, Rsp: c.cfmRsp.Load(), CID: cid, DID: c.did, TimeR: new(uint32)}
	}

	c.lookup, c.reg, c.relay, c.lan, c.pub = newEndpoint(t), newEndpoint(t), newEndpoint(t), newEndpoint(t), newEndpoint(t)

	c.lookup.serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		if _, ok := d.XML.(*C2mQ); ok {
			c.lookups.Add(1)
			e.reply(from, d.TID, &M2cQr{Reg: ipport(c.reg.addr), Relay: ipport(c.relay.addr)})
		}
	})
	c.reg.serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		q, ok := d.XML.(*C2rC)
		if !ok {
			return
		}
		c.registers.Add(1)
		r := &R2cCr{Dev: ipport(c.lan.addr), Dmap: ipport(c.pub.addr), Relay: ipport(c.relay.addr), NAT: "NULL", SID: &c.sid}
		if c.noDmap.Load() {
			r.Dmap = nil
		}
		if c.regFails.Add(-1) >= 0 {
			r.SID, r.Rsp = nil, -3
		}
		if c.refuseEmpty.Load() {
			r = &R2cCr{NAT: "NULL", Rsp: -3}
		}
		_ = q
		e.reply(from, d.TID, r)
	})
	c.lan.serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		if !c.lanAnswers.Load() {
			return
		}
		switch m := d.XML.(type) {
		case *C2dC:
			e.reply(from, d.TID, &D2cCr{Timer: Timer{3000, 20000, 60000}, CID: m.CID, DID: c.did})
		case *C2dT:
			if m.Conn == "local" {
				reply := cfm("local", m.CID)
				if delay := time.Duration(c.lanDelay.Load()); delay > 0 {
					time.AfterFunc(delay, func() { e.reply(from, d.TID, reply) })
				} else {
					e.reply(from, d.TID, reply)
				}
			}
		}
	})
	c.pub.serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		switch m := d.XML.(type) {
		case *C2dT:
			if m.Conn == "map" && c.mapOffers.Load() {
				// the camera connects back from its public address
				e.reply(from, 200, &D2cT{SID: c.sid, Conn: "map", CID: m.CID, DID: c.did})
			}
		case *C2dA:
			if m.Conn == "map" {
				e.reply(from, d.TID, cfm("map", m.CID))
			}
		}
	})
	c.relay.serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		if m, ok := d.XML.(*C2dT); ok && m.Conn == "relay" && c.relayWorks.Load() {
			e.reply(from, d.TID, cfm("relay", m.CID))
		}
	})
	return c
}

// opts point everything at the fake cloud and make time pass quickly.
func (c *cloud) opts(t *testing.T, m Method) Options {
	silent := newEndpoint(t).serve(nil) // the LAN broadcast goes nowhere unless a test sends it to c.lan
	return Options{
		Method:       m,
		LookupHosts:  []string{c.lookup.addr.String()},
		Broadcasts:   []netip.AddrPort{silent.addr},
		LocalIP:      netip.MustParseAddr("192.168.1.2"),
		ReplyTimeout: 2 * time.Second,
		ResendWait:   40 * time.Millisecond,
		MapTimeout:   300 * time.Millisecond,
		BackoffBase:  20 * time.Millisecond,
		BackoffMax:   200 * time.Millisecond,
		NoCache:      true,
		Conn:         fastCfg(),
		Log:          Logger{Debugf: t.Logf, Infof: t.Logf, Warnf: t.Logf},
	}
}

var uidCounter atomic.Int32

func uniqueUID() string { return fmt.Sprintf("TESTUID%08d", uidCounter.Add(1)) }

func discover(t *testing.T, o Options, uid string) (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Discover(ctx, uid, o)
	if c != nil {
		t.Cleanup(func() { c.Close() })
	}
	return c, err
}

// works checks the returned connection carries data both ways with the camera
// that answered at ep.
func works(t *testing.T, c *Conn, ep *endpoint) {
	t.Helper()
	require.Equal(t, ep.addr.String(), c.RemoteAddr().String())

	got := make(chan struct{}, 8)
	ep.setOnData(func(netip.AddrPort) { got <- struct{}{} })
	_, err := c.Write([]byte("ping"))
	require.NoError(t, err)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("camera never saw our data")
	}

	ep.mu.Lock()
	client := ep.client
	ep.mu.Unlock()
	ep.send(client, &Data{ConnID: c.cid, PacketID: 0, Payload: []byte("pong")})
	require.Equal(t, "pong", string(readN(t, c, 4)))
}

func (c *cloud) cfms(conn string) int {
	return c.reg.count("C2R_CFM", func(m Message) bool { return m.(*C2rCfm).Conn == conn })
}

func TestDiscoverLocal(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodLocal)
	o.Broadcasts = []netip.AddrPort{c.lan.addr}

	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.lan)
	require.Equal(t, c.did, conn.did)

	// the query carried the UID, our port and the MTU
	var q *C2dC
	c.lan.mu.Lock()
	for _, m := range c.lan.seen {
		if x, ok := m.(*C2dC); ok {
			q = x
		}
	}
	c.lan.mu.Unlock()
	require.NotNil(t, q)
	require.Equal(t, uint32(MTU), q.MTU)
	require.Equal(t, conn.cid, q.CID)
	require.Equal(t, uint32(conn.sock.Port()), q.Cli.Port)

	// no Reolink server was involved
	require.Zero(t, c.lookups.Load())
}

func TestDiscoverLocalRejectedAnswer(t *testing.T) {
	c := newCloud(t)
	// a camera that answers with rsp -3 is not a success
	bad := newEndpoint(t).serve(func(e *endpoint, d *Discovery, from netip.AddrPort) {
		if m, ok := d.XML.(*C2dC); ok {
			e.reply(from, d.TID, &D2cCr{Rsp: -3, CID: m.CID, DID: 5})
		}
	})
	o := c.opts(t, MethodLocal)
	o.Broadcasts = []netip.AddrPort{bad.addr}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := Discover(ctx, uniqueUID(), o)
	require.Error(t, err)
}

func TestDiscoverRemote(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodRemote)

	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.lan)

	// C2D_T(local) went to the LAN address, the register server was told
	require.GreaterOrEqual(t, c.lan.count("C2D_T", func(m Message) bool { return m.(*C2dT).Conn == "local" }), 1)
	require.Eventually(t, func() bool { return c.cfms("local") == cfmRepeats }, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, c.pub.count("C2D_T", nil), "no punch for the remote method")
}

func TestDiscoverMap(t *testing.T) {
	c := newCloud(t)
	c.lanAnswers.Store(false)
	o := c.opts(t, MethodMap)

	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.pub)
	require.Equal(t, c.did, conn.did)

	require.Equal(t, 1, c.reg.count("C2R_C", nil))
	cl := c.reg.count("C2R_C", func(m Message) bool {
		q := m.(*C2rC)
		return q.Cli.IP == "192.168.1.2" && q.Family == 4 && q.OS == "MAC" && q.R != nil && *q.R == 3 && q.CID == conn.cid &&
			q.Relay == *ipport(c.relay.addr) && q.Cli.Port == uint16(conn.sock.Port())
	})
	require.Equal(t, 1, cl, "registration carries our address, the relay and the client id")

	require.GreaterOrEqual(t, c.pub.count("C2D_T", func(m Message) bool { return m.(*C2dT).Conn == "map" }), 1)
	require.Equal(t, 1, c.pub.count("C2D_A", func(m Message) bool {
		a := m.(*C2dA)
		return a.Conn == "map" && a.DID == c.did && a.CID == conn.cid && a.SID == c.sid && a.MTU == MTU
	}))
	require.Eventually(t, func() bool { return c.cfms("map") == cfmRepeats }, 3*time.Second, 10*time.Millisecond)
	require.Zero(t, c.relay.count("C2D_T", nil), "the relay is not asked by the map method")
}

func TestDiscoverRelayPrefersDirect(t *testing.T) {
	c := newCloud(t)
	c.lanAnswers.Store(false)
	o := c.opts(t, MethodRelay)

	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.pub)
	require.Zero(t, c.relay.count("C2D_T", nil), "a working punch means no relay")
}

func TestDiscoverRelayFallback(t *testing.T) {
	c := newCloud(t)
	c.lanAnswers.Store(false)
	c.mapOffers.Store(false)
	o := c.opts(t, MethodRelay)

	start := time.Now()
	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.relay)
	require.GreaterOrEqual(t, time.Since(start), o.MapTimeout, "the punch got its time first")

	require.Eventually(t, func() bool { return c.cfms("relay") == cfmRepeats }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, c.relay.count("C2D_T", func(m Message) bool {
		x := m.(*C2dT)
		return x.Conn == "relay" && x.SID == c.sid && x.CID == conn.cid && x.MTU == MTU
	}), "")
}

func TestDiscoverRelayMethodUsesLANWhenThere(t *testing.T) {
	// the default method is cumulative: a camera on the LAN wins at once
	c := newCloud(t)
	c.mapOffers.Store(false)
	o := c.opts(t, MethodRelay)
	o.Broadcasts = []netip.AddrPort{c.lan.addr}

	start := time.Now()
	conn, err := discover(t, o, uniqueUID())
	require.NoError(t, err)
	works(t, conn, c.lan)
	require.Less(t, time.Since(start), o.MapTimeout)
}

func TestDiscoverNoPath(t *testing.T) {
	c := newCloud(t)
	c.lanAnswers.Store(false)
	c.mapOffers.Store(false)
	c.relayWorks.Store(false)
	o := c.opts(t, MethodMap)
	o.ReplyTimeout = 200 * time.Millisecond

	_, err := discover(t, o, uniqueUID())
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
}

func TestDiscoverRegistrationRetried(t *testing.T) {
	c := newCloud(t)
	c.regFails.Store(2)
	c.lanAnswers.Store(true)
	o := c.opts(t, MethodRemote)
	uid := uniqueUID()

	conn, err := discover(t, o, uid)
	require.NoError(t, err)
	works(t, conn, c.lan)
	require.GreaterOrEqual(t, c.registers.Load(), int32(3), "two refusals, then the registration worked")
	require.GreaterOrEqual(t, c.lookups.Load(), int32(3), "a new attempt starts over with the lookup")
	require.Zero(t, cooldownWait(uid), "success clears the cooldown")
}

func TestCooldownCarriesOver(t *testing.T) {
	c := newCloud(t)
	c.regFails.Store(1 << 20) // never
	o := c.opts(t, MethodRemote)
	o.BackoffBase = 150 * time.Millisecond
	o.BackoffMax = time.Second
	uid := uniqueUID()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	_, err := Discover(ctx, uid, o)
	require.Error(t, err)

	// attempts at 0, +150, +300 (150*2), ...: not a tight loop
	n := c.registers.Load()
	require.GreaterOrEqual(t, n, int32(2))
	require.LessOrEqual(t, n, int32(4), "backoff grows, no hammering")

	// the next call starts after the cooldown of the previous one
	wait := cooldownWait(uid)
	require.Greater(t, wait, time.Duration(0))
	before := c.lookups.Load()
	ctx2, cancel2 := context.WithTimeout(context.Background(), wait/2)
	defer cancel2()
	_, err = Discover(ctx2, uid, o)
	require.Error(t, err)
	require.Equal(t, before, c.lookups.Load(), "no request to the servers inside the cooldown")

	// the cap
	for i := 0; i < 30; i++ {
		cooldownFail(uid, &o)
	}
	require.LessOrEqual(t, cooldownWait(uid), o.BackoffMax)
	cooldownReset(uid)
	require.Zero(t, cooldownWait(uid))
}

func TestLookupCache(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodRemote)
	o.NoCache = false
	uid := uniqueUID()

	conn, err := discover(t, o, uid)
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, int32(1), c.lookups.Load())

	conn, err = discover(t, o, uid)
	require.NoError(t, err)
	works(t, conn, c.lan)
	require.Equal(t, int32(1), c.lookups.Load(), "the lookup is cached, only the registration is repeated")
	require.Equal(t, int32(2), c.registers.Load())
}

func TestDiscoverCancelled(t *testing.T) {
	c := newCloud(t)
	c.lanAnswers.Store(false)
	c.mapOffers.Store(false)
	c.relayWorks.Store(false)
	o := c.opts(t, MethodRelay)
	o.ReplyTimeout = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := Discover(ctx, uniqueUID(), o)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestDiscoverNoServers(t *testing.T) {
	c := newCloud(t)
	o := c.opts(t, MethodMap)
	o.LookupHosts = []string{"nonexistent.invalid:9999"}
	o.Broadcasts = []netip.AddrPort{c.lan.addr}
	c.lanAnswers.Store(false)
	o.BackoffBase, o.BackoffMax = 10*time.Millisecond, 20*time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := Discover(ctx, uniqueUID(), o)
	require.Error(t, err)
}

func TestLocalDests(t *testing.T) {
	o := Options{Addr: "192.168.1.50"}
	got, err := o.localDests()
	require.NoError(t, err)
	require.Equal(t, []netip.AddrPort{netip.MustParseAddrPort("192.168.1.50:2018"), netip.MustParseAddrPort("192.168.1.50:2015")}, got,
		"straight to the camera instead of broadcasting")

	_, err = (&Options{Addr: "not an ip"}).localDests()
	require.Error(t, err)

	got, err = (&Options{}).localDests()
	require.NoError(t, err)
	require.Contains(t, got, netip.MustParseAddrPort("255.255.255.255:2015"))
	require.Contains(t, got, netip.MustParseAddrPort("255.255.255.255:2018"))
	for _, d := range got {
		require.Contains(t, []uint16{2015, 2018}, d.Port())
		require.True(t, d.Addr().Is4())
	}
}

func TestMethodAndRegion(t *testing.T) {
	for s, want := range map[string]Method{"": MethodRelay, "relay": MethodRelay, "map": MethodMap, "remote": MethodRemote, "local": MethodLocal} {
		m, ok := ParseMethod(s)
		require.True(t, ok)
		require.Equal(t, want, m)
	}
	_, ok := ParseMethod("telepathy")
	require.False(t, ok)
	require.Equal(t, "relay", MethodRelay.String())

	require.Equal(t, "p2p2.reolink.com:9999", regionHost(" Europe (Germany) "))
	require.Equal(t, "p2p8.reolink.com:9999", regionHost("Europe (Unite Kingdom)"))
	require.Equal(t, "", regionHost("Mars"))

	o := Options{Region: "Middle East"}
	o.setDefaults()
	require.Equal(t, []string{"p2p6.reolink.com:9999"}, o.LookupHosts)
	o = Options{}
	o.setDefaults()
	require.Len(t, o.LookupHosts, 12)
	require.Equal(t, "p2p.reolink.com:9999", o.LookupHosts[0])
	require.Equal(t, "p2p11.reolink.com:9999", o.LookupHosts[11])
}

func TestSessionRoutingByTID(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	s, err := newSession(o)
	require.NoError(t, err)
	defer s.close()

	ch, unsub, err := s.subscribe(7)
	require.NoError(t, err)
	_, _, err = s.subscribe(7)
	require.Error(t, err, "one waiter per tid")
	all, unlisten := s.listen()
	defer unlisten()

	peer := netip.MustParseAddrPort("127.0.0.1:9")
	b7, _ := (&Discovery{TID: 7, XML: &D2cHb{}}).Marshal()
	b8, _ := (&Discovery{TID: 8, XML: &D2cHb{}}).Marshal()
	s.onPacket(b7, peer)
	s.onPacket(b8, peer)
	s.onPacket([]byte("junk"), peer)
	d, _ := (&Data{Payload: []byte{1}}).Marshal()
	s.onPacket(d, peer)

	require.Len(t, ch, 1, "tid 7 goes to its waiter")
	require.Len(t, all, 1, "the rest goes to the listeners, not tid 7")
	unsub()
	s.onPacket(b7, peer)
	require.Len(t, all, 2)

	for i := 0; i < 1000; i++ {
		require.NotZero(t, randTID())
		require.LessOrEqual(t, randTID(), uint32(255))
	}
}
