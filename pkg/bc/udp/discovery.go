package udp

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Method is how far Discover goes to reach the camera. The methods are
// cumulative, each one tries everything the previous ones do:
//
//	local   LAN broadcast to the UID (and straight to Options.Addr)
//	remote  + the camera's LAN address from the register server
//	map     + hole punching to its public address
//	relay   + the Reolink relay server as the last resort; the punch gets
//	          3 s first, the relay only forwards when no direct path works
//
// The zero value is relay, the default of the official client.
type Method uint8

const (
	MethodRelay Method = iota
	MethodMap
	MethodRemote
	MethodLocal
)

func ParseMethod(s string) (Method, bool) {
	switch s {
	case "", "relay":
		return MethodRelay, true
	case "map":
		return MethodMap, true
	case "remote":
		return MethodRemote, true
	case "local":
		return MethodLocal, true
	}
	return 0, false
}

func (m Method) String() string {
	return [...]string{"relay", "map", "remote", "local"}[m]
}

// Options of Discover. Zero values are the defaults of the official client;
// the tuning fields exist for tests.
type Options struct {
	Method Method
	// Addr is the camera's IP if known: local discovery then goes straight
	// to it (ports 2018 and 2015) instead of broadcasting.
	Addr string
	// Region selects a single lookup server (e.g. "Europe (Germany)"), all
	// are asked in parallel by default.
	Region string
	Log    Logger
	Conn   ConnConfig

	LookupHosts  []string         // host:port of the lookup servers, p2p*.reolink.com:9999
	Broadcasts   []netip.AddrPort // where the LAN query goes, interface broadcasts on 2015 and 2018
	LocalIP      netip.Addr       // reported to the register server, the first non-loopback IPv4
	ReplyTimeout time.Duration    // each request, 15 s
	ResendWait   time.Duration    // requests are repeated, 500 ms
	MapTimeout   time.Duration    // the relay method waits this long for the punch, 3 s
	BackoffBase  time.Duration    // registration retry delay, 1 s doubling
	BackoffMax   time.Duration    // 30 s
	NoCache      bool             // no lookup cache
}

const (
	lookupCacheTTL = 5 * time.Minute
	cfmRepeats     = 5
)

var defaultLookupHosts = func() []string {
	hosts := []string{"p2p.reolink.com:9999"}
	for i := 1; i <= 11; i++ {
		hosts = append(hosts, fmt.Sprintf("p2p%d.reolink.com:9999", i))
	}
	return hosts
}()

func regionHost(region string) string {
	switch strings.ToLower(strings.TrimSpace(region)) {
	case "north america (east us)":
		return "p2p1.reolink.com:9999"
	case "europe (germany)":
		return "p2p2.reolink.com:9999"
	case "asia (hong kong)":
		return "p2p3.reolink.com:9999"
	case "middle east":
		return "p2p6.reolink.com:9999"
	case "europe (france)":
		return "p2p7.reolink.com:9999"
	case "europe (united kingdom)", "europe (unite kingdom)":
		return "p2p8.reolink.com:9999"
	case "north america (west us)":
		return "p2p9.reolink.com:9999"
	}
	return ""
}

func (o *Options) setDefaults() {
	o.Log = o.Log.Filled()
	if o.ReplyTimeout == 0 {
		o.ReplyTimeout = 15 * time.Second
	}
	if o.ResendWait == 0 {
		o.ResendWait = 500 * time.Millisecond
	}
	if o.MapTimeout == 0 {
		o.MapTimeout = 3 * time.Second
	}
	if o.BackoffBase == 0 {
		o.BackoffBase = time.Second
	}
	if o.BackoffMax == 0 {
		o.BackoffMax = 30 * time.Second
	}
	if o.Conn.Log.empty() {
		o.Conn.Log = o.Log
	}
	if len(o.LookupHosts) == 0 {
		if h := regionHost(o.Region); h != "" {
			o.LookupHosts = []string{h}
		} else {
			o.LookupHosts = defaultLookupHosts
		}
	}
}

// --- process wide state: the lookup cache and the registration cooldown

type cachedLookup struct {
	lk lookupInfo
	at time.Time
}

type cooldown struct {
	fails int
	until time.Time
}

var state struct {
	mu       sync.Mutex
	lookups  map[string]cachedLookup
	cooldown map[string]*cooldown
}

// cacheKey is the camera and the lookup policy (the servers that are asked,
// which the region selects): a lookup made under another policy is not reused.
func cacheKey(uid string, o *Options) string {
	return uid + "|" + strings.Join(o.LookupHosts, ",")
}

func cacheGet(key string) (lookupInfo, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	c, ok := state.lookups[key]
	if !ok || time.Since(c.at) > lookupCacheTTL {
		delete(state.lookups, key)
		return lookupInfo{}, false
	}
	return c.lk, true
}

func cachePut(key string, lk lookupInfo) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lookups == nil {
		state.lookups = map[string]cachedLookup{}
	}
	state.lookups[key] = cachedLookup{lk, time.Now()}
}

// cooldownWait is how long the next attempt for the uid has to wait. The
// official app and the servers punish rapid reconnects with a cooldown of a
// minute or two, so a failure delays the next try: 1, 2, 4 ... 60 s.
func cooldownWait(uid string) time.Duration {
	state.mu.Lock()
	defer state.mu.Unlock()
	if c := state.cooldown[uid]; c != nil {
		return time.Until(c.until)
	}
	return 0
}

func cooldownFail(uid string, o *Options) (wait time.Duration) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cooldown == nil {
		state.cooldown = map[string]*cooldown{}
	}
	c := state.cooldown[uid]
	if c == nil {
		c = new(cooldown)
		state.cooldown[uid] = c
	}
	wait = o.BackoffBase << min(c.fails, 20)
	if wait > o.BackoffMax || wait <= 0 {
		wait = o.BackoffMax
	}
	c.fails++
	c.until = time.Now().Add(wait)
	return wait
}

func cooldownReset(uid string) {
	state.mu.Lock()
	delete(state.cooldown, uid)
	state.mu.Unlock()
}

// --- results

type lookupInfo struct {
	reg, relay netip.AddrPort
	via        netip.AddrPort
}

type regResult struct {
	reg              netip.AddrPort
	dev, dmap, relay netip.AddrPort // zero if not offered
	sid              uint32
}

type found struct {
	sess *session
	peer netip.AddrPort
	did  int32
	sid  uint32 // the relay session, 0 on the LAN
	path string // local, map, relay: what the media path is
}

// Discover finds the camera with the given UID and returns the connection to
// it. Paths race, the first that is confirmed wins; see Method. It gives up
// when ctx ends. Failed registrations are retried with a growing delay that
// also carries over to the next call for the same UID.
func Discover(ctx context.Context, uid string, opts Options) (*Conn, error) {
	opts.setDefaults()
	o := &opts

	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wantRemote := o.Method != MethodLocal
	wantMap := o.Method == MethodMap
	wantRelay := o.Method == MethodRelay

	type outcome struct {
		f   *found
		err error
	}
	out := make(chan outcome, 2)
	paths := 0
	var counted atomic.Bool   // this call already added to the cooldown
	var attempted atomic.Bool // this call really contacted the servers

	// LAN
	paths++
	go func() {
		s, err := newSession(o)
		if err != nil {
			out <- outcome{err: err}
			return
		}
		f, err := s.local(ctx, uid)
		if err != nil {
			s.close()
		}
		out <- outcome{f, err}
	}()

	// register, then remote / map / relay
	if wantRemote {
		paths++
		go func() {
			s, reg, err := registerLoop(ctx, uid, o, &counted, &attempted)
			if err != nil {
				out <- outcome{err: err}
				return
			}
			f, err := s.race(ctx, reg, wantRemote, wantMap, wantRelay)
			if err != nil {
				s.close()
				if ctx.Err() == nil {
					counted.Store(true)
					cooldownFail(uid, o)
				}
			}
			out <- outcome{f, err}
		}()
	}

	var winner *found
	var errs []error
	var rest []outcome
	for i := 0; i < paths; i++ {
		r := <-out
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		if winner == nil {
			winner = r.f
			cancel() // the others stop
			continue
		}
		rest = append(rest, r) // finished at the same moment
	}
	for _, r := range rest {
		r.f.sess.close()
	}

	if winner == nil {
		joined := errors.Join(errs...)
		var cd *CooldownError
		if errors.As(joined, &cd) && !attempted.Load() {
			return nil, cd // only waited: no failure of the camera, nothing to count
		}

		err := parent.Err()
		if err == nil {
			err = fmt.Errorf("udp: camera %s not reachable: %w", uid, joined)
		}
		// running out of time after the servers were contacted is a failed
		// attempt like any other and counts for the cooldown (once per call);
		// a cancel by the caller is exempt, and so is a call that only waited
		if !errors.Is(err, context.Canceled) && !counted.Load() && attempted.Load() && o.Method != MethodLocal {
			cooldownFail(uid, o)
		}
		return nil, err
	}

	cooldownReset(uid)
	switch winner.path {
	case "relay":
		o.Log.Infof("media path: routed through the Reolink relay at %s (not direct)", winner.peer)
	case "map":
		o.Log.Infof("media path: direct P2P to the camera's public address %s", winner.peer)
	default:
		o.Log.Infof("media path: direct to the camera at %s", winner.peer)
	}
	cc := o.Conn
	cc.SID = winner.sid
	return newConn(winner.sess.sock, true, winner.peer, winner.sess.cid, winner.did, cc), nil
}

// registerLoop registers with the Reolink servers, again and again with a
// growing delay: the servers only learn about a new connection attempt lazily
// and the first tries fail.
func registerLoop(ctx context.Context, uid string, o *Options, counted, attempted *atomic.Bool) (*session, *regResult, error) {
	for {
		// the cooldown is looked at again after every wait: another call for the
		// same camera may have extended it meanwhile
		for wait := cooldownWait(uid); wait > 0; wait = cooldownWait(uid) {
			// no point in spending the whole call on waiting
			if dl, ok := ctx.Deadline(); ok && wait > time.Until(dl) {
				return nil, nil, &CooldownError{Wait: wait}
			}
			o.Log.Debugf("waiting %s before contacting the Reolink servers again", wait.Round(time.Millisecond))
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}

		attempted.Store(true)
		s, err := newSession(o)
		if err != nil {
			return nil, nil, err
		}
		reg, err := s.getRegistration(ctx, uid)
		if err == nil {
			return s, reg, nil
		}
		s.close()
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		counted.Store(true)
		wait := cooldownFail(uid, o)
		o.Log.Warnf("registration failed (%v), retrying in %s", err, wait)
	}
}

// --- session: one socket, one client id, the discovery traffic

type reply struct {
	d    *Discovery
	from netip.AddrPort
}

type session struct {
	o    *Options
	sock *sock
	cid  int32

	mu       sync.Mutex
	subs     map[uint32]chan reply // by tid
	handlers map[chan reply]struct{}

	won atomic.Bool // a path has been chosen
}

func newSession(o *Options) (*session, error) {
	sk, err := listenSock()
	if err != nil {
		return nil, err
	}
	s := &session{
		o: o, sock: sk, cid: int32(rand.Uint32()),
		subs: map[uint32]chan reply{}, handlers: map[chan reply]struct{}{},
	}
	sk.setHandler(s.onPacket)
	return s, nil
}

func (s *session) close() { _ = s.sock.Close() }

// onPacket routes a discovery message. A request gets the messages with its
// tid; offers and confirmations also go to the listeners, because the tid
// space is small (one byte in practice) and an unrelated request may use the
// tid of a message from the camera. Everything nobody subscribed to goes to
// the listeners.
func (s *session) onPacket(b []byte, from netip.AddrPort) {
	p, err := Decode(b)
	if err != nil {
		return
	}
	d, ok := p.(*Discovery)
	if !ok {
		return
	}

	s.mu.Lock()
	var to []chan reply
	ch := s.subs[d.TID]
	if ch != nil && d.TID > 0 {
		to = append(to, ch)
	}
	switch d.XML.(type) {
	case *D2cT, *D2cCfm:
		ch = nil // a waiter may not be able to use it
	}
	if ch == nil || d.TID == 0 {
		for l := range s.handlers {
			to = append(to, l)
		}
	}
	s.mu.Unlock()

	for _, ch := range to {
		select {
		case ch <- reply{d, from}:
		default: // a listener that does not keep up misses it, requests are repeated
		}
	}
}

// randTID is a tid for a message nobody answers.
func randTID() uint32 { return uint32(rand.IntN(255) + 1) }

// subscribe takes a tid the caller was given (the camera's, for an accept).
func (s *session) subscribe(tid uint32) (ch chan reply, unsub func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscribeLocked(tid)
}

func (s *session) subscribeLocked(tid uint32) (ch chan reply, unsub func(), err error) {
	if s.subs[tid] != nil {
		return nil, nil, fmt.Errorf("udp: tid %d is already in use", tid)
	}
	ch = make(chan reply, 16)
	s.subs[tid] = ch
	return ch, func() {
		s.mu.Lock()
		if s.subs[tid] == ch {
			delete(s.subs, tid)
		}
		s.mu.Unlock()
	}, nil
}

// subscribeNew picks a free tid and reserves it in one step. The tries are
// bounded, an exhausted space is an error and not a loop under the mutex.
func (s *session) subscribeNew() (tid uint32, ch chan reply, unsub func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for try := 0; try < 1024; try++ {
		tid = randTID()
		if s.subs[tid] == nil {
			ch, unsub, err = s.subscribeLocked(tid)
			return
		}
	}
	return 0, nil, nil, errors.New("udp: no free tid")
}

// listen receives all discovery traffic nobody subscribed to.
func (s *session) listen() (ch chan reply, unlisten func()) {
	ch = make(chan reply, 128)
	s.mu.Lock()
	s.handlers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.handlers, ch)
		s.mu.Unlock()
	}
}

func (s *session) send(d *Discovery, to netip.AddrPort) {
	if b, err := d.Marshal(); err == nil {
		_ = s.sock.send(b, to)
	}
}

// request sends the message every ResendWait until match accepts a reply,
// the reply timeout, or ctx. With anyTid, replies that do not echo the tid
// count too (the camera answers an accept under its own tid).
func request[T any](ctx context.Context, s *session, d *Discovery, to netip.AddrPort, anyTid bool, match func(*Discovery, netip.AddrPort) (T, bool)) (T, error) {
	var zero T
	var ch chan reply
	var unsub func()
	var err error
	if d.TID == 0 {
		d.TID, ch, unsub, err = s.subscribeNew()
	} else {
		ch, unsub, err = s.subscribe(d.TID)
		if err != nil && anyTid {
			// the camera chose this tid and an unrelated request has it: the
			// answer is found by its content among what the listeners get
			ch, unsub, err = nil, func() {}, nil
		}
	}
	if err != nil {
		return zero, err
	}
	defer unsub()

	var other chan reply
	if anyTid {
		var unlisten func()
		other, unlisten = s.listen()
		defer unlisten()
	}

	wire, err := d.Marshal()
	if err != nil {
		return zero, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.o.ReplyTimeout)
	defer cancel()
	tick := time.NewTicker(s.o.ResendWait)
	defer tick.Stop()

	_ = s.sock.send(wire, to)
	for {
		var r reply
		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("%s to %s: %w", d.XML.XMLName(), to, ctxErr(ctx))
		case <-tick.C:
			_ = s.sock.send(wire, to)
			continue
		case r = <-ch:
		case r = <-other:
		}
		if v, ok := match(r.d, r.from); ok {
			return v, nil
		}
	}
}

func ctxErr(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrNoReply
	}
	return ctx.Err()
}

// CooldownError is returned when an earlier failure for the same camera still
// holds the servers off for longer than the call may take. Nothing was sent
// and nothing is counted as a new failure; the caller retries later.
type CooldownError struct{ Wait time.Duration }

func (e *CooldownError) Error() string {
	return fmt.Sprintf("udp: waiting for the P2P cooldown, retry in %s", e.Wait.Round(time.Second))
}

var (
	ErrNoReply       = errors.New("no reply")
	ErrRegisterFail  = errors.New("udp: the register server refused")
	ErrConfirmFailed = errors.New("udp: the camera refused the connection")
	errPathCompleted = errors.New("udp: another path already completed")
)

// --- LAN

// localDests are where the LAN query goes: the camera itself if its address
// is known, else the broadcast addresses.
func (o *Options) localDests() ([]netip.AddrPort, error) {
	if o.Addr != "" {
		ip, err := netip.ParseAddr(o.Addr)
		if err != nil {
			return nil, fmt.Errorf("udp: bad camera address %q", o.Addr)
		}
		return []netip.AddrPort{netip.AddrPortFrom(ip, 2018), netip.AddrPortFrom(ip, 2015)}, nil
	}
	if len(o.Broadcasts) > 0 {
		return o.Broadcasts, nil
	}
	return broadcasts(), nil
}

func (s *session) local(ctx context.Context, uid string) (*found, error) {
	dests, err := s.o.localDests()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		f   *found
		err error
	}
	out := make(chan res, len(dests))
	for _, dest := range dests {
		go func() {
			msg := &Discovery{XML: &C2dC{
				UID: uid, Cli: Port{uint32(s.sock.Port())}, CID: s.cid, MTU: MTU, OS: "MAC",
			}}
			f, err := request(ctx, s, msg, dest, false, func(d *Discovery, from netip.AddrPort) (*found, bool) {
				if r, ok := d.XML.(*D2cCr); ok && r.CID == s.cid && r.Rsp == 0 {
					return &found{sess: s, peer: from, did: r.DID, path: "local"}, true
				}
				return nil, false
			})
			out <- res{f, err}
		}()
	}

	var errs []error
	for range dests {
		r := <-out
		if r.err == nil {
			return r.f, nil
		}
		errs = append(errs, r.err)
	}
	return nil, errors.Join(errs...)
}

// --- lookup and registration

// lookup asks one lookup server where the register and relay servers are.
func (s *session) lookup(ctx context.Context, uid string, server netip.AddrPort) (lookupInfo, error) {
	msg := &Discovery{XML: &C2mQ{UID: uid, OS: "MAC"}}
	return request(ctx, s, msg, server, false, func(d *Discovery, _ netip.AddrPort) (lookupInfo, bool) {
		if q, ok := d.XML.(*M2cQr); ok && q.Reg.Valid() && q.Relay.Valid() {
			reg, _ := q.Reg.Addr()
			relay, _ := q.Relay.Addr()
			return lookupInfo{reg: reg, relay: relay, via: server}, true
		}
		return lookupInfo{}, false
	})
}

func (s *session) registerAddress(ctx context.Context, uid string, lk lookupInfo) (*regResult, error) {
	ip := s.o.LocalIP
	if !ip.IsValid() {
		ip = localIPv4()
	}
	rev := int32(3)
	msg := &Discovery{XML: &C2rC{
		UID:    uid,
		Cli:    IpPort{IP: ip.String(), Port: uint16(s.sock.Port())},
		Relay:  IpPort{IP: lk.relay.Addr().String(), Port: lk.relay.Port()},
		CID:    s.cid,
		Family: 4,
		OS:     "MAC",
		R:      &rev,
	}}

	type reg struct {
		r   *regResult
		err error
	}
	got, err := request(ctx, s, msg, lk.reg, false, func(d *Discovery, _ netip.AddrPort) (reg, bool) {
		r, ok := d.XML.(*R2cCr)
		if !ok {
			return reg{}, false
		}
		// a refusal is a refusal with or without addresses
		if r.Rsp == -1 || r.Rsp == -3 {
			return reg{err: ErrRegisterFail}, true
		}
		if (r.Dev.Valid() || r.Dmap.Valid() || r.Relay.Valid()) && r.SID != nil {
			res := &regResult{reg: lk.reg, sid: *r.SID}
			res.dev, _ = r.Dev.Addr()
			res.dmap, _ = r.Dmap.Addr()
			res.relay, _ = r.Relay.Addr()
			return reg{r: res}, true
		}
		return reg{}, false
	})
	if err != nil {
		return nil, err
	}
	return got.r, got.err
}

// getRegistration finds the camera at the Reolink servers: the cached lookup
// first, then all lookup servers in parallel, each answer registered with its
// register server; the first registration that works wins.
func (s *session) getRegistration(ctx context.Context, uid string) (*regResult, error) {
	if !s.o.NoCache {
		if lk, ok := cacheGet(cacheKey(uid, s.o)); ok {
			s.o.Log.Debugf("using cached lookup for %s", uid)
			if r, err := s.registerAddress(ctx, uid, lk); err == nil {
				return r, nil
			}
			s.o.Log.Debugf("registration with the cached lookup failed, asking again")
		}
	}

	var servers []netip.AddrPort
	for _, h := range s.o.LookupHosts {
		servers = append(servers, resolve(ctx, h)...)
	}
	if len(servers) == 0 {
		return nil, errors.New("udp: cannot resolve the Reolink lookup servers")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		r   *regResult
		lk  lookupInfo
		err error
	}
	out := make(chan res)
	var wg sync.WaitGroup
	var seen sync.Map // register servers already asked

	for _, server := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lk, err := s.lookup(ctx, uid, server)
			if err == nil {
				if _, dup := seen.LoadOrStore(lk.reg, true); dup {
					err = errors.New("register server already asked")
				}
			}
			if err != nil {
				select {
				case out <- res{err: err}:
				case <-ctx.Done():
				}
				return
			}
			r, err := s.registerAddress(ctx, uid, lk)
			select {
			case out <- res{r, lk, err}:
			case <-ctx.Done():
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()

	var errs []error
	for r := range out {
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		if !s.o.NoCache {
			cachePut(cacheKey(uid, s.o), r.lk)
		}
		s.o.Log.Debugf("registered with %s (relay %s)", r.lk.reg, r.lk.relay)
		return r.r, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("udp: no Reolink register server returned the camera: %w", errors.Join(errs...))
}

func resolve(ctx context.Context, hostport string) []netip.AddrPort {
	if ap, err := netip.ParseAddrPort(hostport); err == nil {
		return []netip.AddrPort{ap}
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil
	}
	p, _ := netip.ParseAddrPort("0.0.0.0:" + port)
	var out []netip.AddrPort
	for _, ip := range ips {
		out = append(out, netip.AddrPortFrom(ip.Unmap(), p.Port()))
	}
	return out
}

// --- the paths after registration

// race starts the enabled paths: remote at once, map after 250 ms and relay
// after 500 ms (so the closer paths get a head start), the first one that is
// confirmed wins.
func (s *session) race(ctx context.Context, reg *regResult, remote, mapp, relay bool) (*found, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		f   *found
		err error
	}
	out := make(chan res, 3)
	n := 0
	start := func(delay time.Duration, name string, f func(context.Context, *regResult) (*found, error)) {
		n++
		go func() {
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					out <- res{err: ctx.Err()}
					return
				}
			}
			s.o.Log.Debugf("trying %s discovery", name)
			r, err := f(ctx, reg)
			if err != nil {
				err = fmt.Errorf("%s: %w", name, err)
			}
			out <- res{r, err}
		}()
	}
	if remote {
		start(0, "remote", s.remote)
	}
	if mapp {
		start(250*time.Millisecond, "map", func(ctx context.Context, reg *regResult) (*found, error) {
			return s.deviceInitiatedMap(ctx, reg, s.o.ReplyTimeout)
		})
	}
	if relay {
		start(500*time.Millisecond, "relay", s.relayAssisted)
	}

	var errs []error
	for i := 0; i < n; i++ {
		r := <-out
		if r.err == nil {
			return r.f, nil
		}
		errs = append(errs, r.err)
	}
	return nil, errors.Join(errs...)
}

// complete is the common end of every path, called when the path is fully
// confirmed. Only this step is exclusive: the first confirmed path becomes the
// result at once (nothing waits behind a path that is still negotiating) and
// the register server is told which path it was (several times, UDP).
func (s *session) complete(reg *regResult, conn string, peer netip.AddrPort, did int32) (*found, error) {
	if !s.won.CompareAndSwap(false, true) {
		return nil, errPathCompleted
	}
	go func() {
		for i := 0; i < cfmRepeats; i++ {
			if i > 0 {
				time.Sleep(s.o.ResendWait)
			}
			s.send(&Discovery{TID: randTID(), XML: &C2rCfm{SID: reg.sid, Conn: conn, CID: s.cid, DID: did}}, reg.reg)
		}
	}()
	return &found{sess: s, peer: peer, did: did, sid: reg.sid, path: conn}, nil
}

// confirmation is a D2C_CFM that belongs to a request.
type confirmation struct {
	from netip.AddrPort
	did  int32
	rsp  int32
}

// check turns a confirmation into success or the camera's refusal.
func (c confirmation) check(conn string) error {
	if c.rsp != 0 {
		return fmt.Errorf("%w (%s, rsp %d)", ErrConfirmFailed, conn, c.rsp)
	}
	return nil
}

// remote races the two ways to the camera's LAN address: we ask it, or it
// asks us.
func (s *session) remote(ctx context.Context, reg *regResult) (*found, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		f   *found
		err error
	}
	out := make(chan res, 2)
	go func() { f, err := s.clientInitiatedDev(ctx, reg); out <- res{f, err} }()
	go func() { f, err := s.deviceInitiatedDev(ctx, reg); out <- res{f, err} }()

	var errs []error
	for i := 0; i < 2; i++ {
		r := <-out
		if r.err == nil {
			return r.f, nil
		}
		errs = append(errs, r.err)
	}
	return nil, errors.Join(errs...)
}

func (s *session) clientInitiatedDev(ctx context.Context, reg *regResult) (*found, error) {
	if !reg.dev.IsValid() {
		return nil, errors.New("no device address")
	}
	msg := &Discovery{XML: &C2dT{SID: reg.sid, Conn: "local", CID: s.cid, MTU: MTU}}
	r, err := request(ctx, s, msg, reg.dev, false, func(d *Discovery, from netip.AddrPort) (confirmation, bool) {
		if m, ok := d.XML.(*D2cCfm); ok && m.CID == s.cid && m.SID == reg.sid && m.Conn == "local" {
			return confirmation{from, m.DID, m.Rsp}, true
		}
		return confirmation{}, false
	})
	if err != nil {
		return nil, err
	}
	if err = r.check("local"); err != nil {
		return nil, err
	}
	return s.complete(reg, "local", r.from, r.did)
}

// accept waits for the camera to offer a connection (D2C_T) matching ok,
// answers with C2D_A under the camera's tid and waits for D2C_CFM.
func (s *session) accept(ctx context.Context, reg *regResult, conn string, offered <-chan reply, ok func(*D2cT, netip.AddrPort) bool) (*found, error) {
	var offer *D2cT
	var from netip.AddrPort
	var tid uint32
wait:
	for {
		select {
		case r := <-offered:
			if t, isT := r.d.XML.(*D2cT); isT && t.CID == s.cid && t.SID == reg.sid && t.Conn == conn && ok(t, r.from) {
				offer, from, tid = t, r.from, r.d.TID
				break wait
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("no offer from the camera: %w", ctxErr(ctx))
		}
	}

	msg := &Discovery{TID: tid, XML: &C2dA{SID: reg.sid, Conn: conn, CID: s.cid, DID: offer.DID, MTU: MTU}}
	r, err := request(ctx, s, msg, from, true, func(d *Discovery, _ netip.AddrPort) (confirmation, bool) {
		m, isCfm := d.XML.(*D2cCfm)
		if isCfm && m.SID == reg.sid && m.DID == offer.DID && m.CID == s.cid && m.Conn == conn {
			return confirmation{from, m.DID, m.Rsp}, true
		}
		return confirmation{}, false
	})
	if err != nil {
		return nil, err
	}
	if err = r.check(conn); err != nil {
		return nil, err
	}
	return s.complete(reg, conn, from, offer.DID)
}

func (s *session) deviceInitiatedDev(ctx context.Context, reg *regResult) (*found, error) {
	ctx, cancel := context.WithTimeout(ctx, s.o.ReplyTimeout)
	defer cancel()
	offered, unlisten := s.listen()
	defer unlisten()
	return s.accept(ctx, reg, "local", offered, func(_ *D2cT, from netip.AddrPort) bool {
		return reg.dmap.IsValid() && from == reg.dmap
	})
}

// deviceInitiatedMap is the hole punch: we send a few C2D_T to the camera's
// public address, which opens our NAT towards it and tells the camera to
// connect back with a D2C_T from that address (the port may differ).
func (s *session) deviceInitiatedMap(ctx context.Context, reg *regResult, timeout time.Duration) (*found, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	offered, unlisten := s.listen()
	defer unlisten()

	if reg.dmap.IsValid() {
		go func() {
			tk := time.NewTicker(s.o.ResendWait)
			defer tk.Stop()
			for i := 0; i < 5; i++ {
				s.send(&Discovery{TID: randTID(), XML: &C2dT{SID: reg.sid, Conn: "map", CID: s.cid, MTU: MTU}}, reg.dmap)
				select {
				case <-tk.C:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return s.accept(ctx, reg, "map", offered, func(_ *D2cT, from netip.AddrPort) bool {
		if !reg.dmap.IsValid() {
			return true
		}
		if from.Addr() != reg.dmap.Addr() {
			s.o.Log.Debugf("map offer from %s, expected %s", from, reg.dmap)
			return false
		}
		return true
	})
}

func (s *session) clientInitiatedRelay(ctx context.Context, reg *regResult) (*found, error) {
	if !reg.relay.IsValid() {
		return nil, errors.New("no relay address")
	}
	msg := &Discovery{XML: &C2dT{SID: reg.sid, Conn: "relay", CID: s.cid, MTU: MTU}}
	r, err := request(ctx, s, msg, reg.relay, false, func(d *Discovery, from netip.AddrPort) (confirmation, bool) {
		if m, ok := d.XML.(*D2cCfm); ok && m.CID == s.cid && m.SID == reg.sid && m.Conn == "relay" {
			return confirmation{from, m.DID, m.Rsp}, true
		}
		return confirmation{}, false
	})
	if err != nil {
		return nil, err
	}
	if err = r.check("relay"); err != nil {
		return nil, err
	}
	return s.complete(reg, "relay", r.from, r.did)
}

// relayAssisted prefers a direct path: the hole punch gets MapTimeout, the
// relay only forwards the traffic when that fails.
func (s *session) relayAssisted(ctx context.Context, reg *regResult) (*found, error) {
	if reg.dmap.IsValid() {
		f, err := s.deviceInitiatedMap(ctx, reg, s.o.MapTimeout)
		if err == nil {
			return f, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		s.o.Log.Infof("hole punch failed (%v), using the relay", err)
	} else {
		s.o.Log.Infof("no public address for the hole punch, using the relay")
	}
	return s.clientInitiatedRelay(ctx, reg)
}

// --- addresses

func localIPv4() netip.Addr {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() {
			if ip, ok := netip.AddrFromSlice(n.IP.To4()); ok {
				return ip
			}
		}
	}
	return netip.AddrFrom4([4]byte{127, 0, 0, 1})
}

// broadcasts are the broadcast addresses of the interfaces and the global one
// on the ports the cameras listen to.
func broadcasts() []netip.AddrPort {
	ips := []netip.Addr{netip.AddrFrom4([4]byte{255, 255, 255, 255})}
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok || n.IP.To4() == nil || len(n.Mask) != net.IPv4len && len(n.Mask) != net.IPv6len {
			continue
		}
		ip, mask := n.IP.To4(), n.Mask
		if len(mask) == net.IPv6len {
			mask = mask[12:]
		}
		var b [4]byte
		for i := range b {
			b[i] = ip[i] | ^mask[i]
		}
		if bc := netip.AddrFrom4(b); !bc.IsLoopback() && !slicesContains(ips, bc) {
			ips = append(ips, bc)
		}
	}

	var out []netip.AddrPort
	for _, ip := range ips {
		out = append(out, netip.AddrPortFrom(ip, 2015), netip.AddrPortFrom(ip, 2018))
	}
	return out
}

func slicesContains(s []netip.Addr, v netip.Addr) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
