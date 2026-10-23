package udp

import (
	"errors"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"
)

const (
	// MTU is announced to the camera, a datagram is at most this big.
	MTU = 1350
	// dataHeader is the size of the Data header, the payload gets the rest.
	dataHeader = 20
	// MaxPayload is the stream bytes carried by one Data packet.
	MaxPayload = MTU - dataHeader

	maxSACK = 1200 // selective ACK bytes, keeps the ACK inside one datagram
)

var (
	ErrClosed     = errors.New("udp: connection closed")
	ErrTimeout    = errors.New("udp: camera stopped sending")
	ErrPeerClosed = errors.New("udp: camera ended the connection")
	ErrRelayEnded = errors.New("udp: relay ended the connection")
	ErrWindow     = errors.New("udp: camera does not acknowledge, send window full")
)

// ConnConfig tunes a Conn, zero values are what the official client does.
type ConnConfig struct {
	GapSkip        time.Duration // wait for a missing packet before skipping it, 500 ms
	Timeout        time.Duration // silence from the camera that kills the connection, 10 s
	ResendInterval time.Duration // unacknowledged packets are sent again, 500 ms
	AckInterval    time.Duration // ACK cadence, the camera drops us without, 10 ms
	HBInterval     time.Duration // C2D_HB cadence, 1 s
	MaxUnacked     int           // sent packets waiting for an ACK before Write blocks, 4096
	WriteStall     time.Duration // how long Write waits for window space, 10 s
	RxLimit        int           // bytes received but not read before the oldest are dropped, 16 MiB
	Log            Logger
}

func (c *ConnConfig) setDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.GapSkip, 500*time.Millisecond)
	def(&c.Timeout, 10*time.Second)
	def(&c.ResendInterval, 500*time.Millisecond)
	def(&c.AckInterval, 10*time.Millisecond)
	def(&c.HBInterval, time.Second)
	def(&c.WriteStall, 10*time.Second)
	if c.MaxUnacked == 0 {
		c.MaxUnacked = 4096
	}
	if c.RxLimit == 0 {
		c.RxLimit = 16 << 20
	}
	c.Log = c.Log.Filled()
}

// Stats are counters for diagnostics and tests.
type Stats struct {
	Resends     int    // packets sent again
	GapSkips    int    // times a missing packet was given up on
	Reordered   int    // packets that arrived ahead of the contiguous point
	Dropped     int    // received stream bytes dropped because nobody reads
	RecvRate    uint32 // bytes/s reported to the camera in the ACKs
	Rebinds     int64  // socket rebinds after a send stall
	PacketsWant uint32 // next packet awaited
	Unacked     int    // sent packets without ACK
}

// Conn is the reliable byte stream to a camera over UDP: the Baichuan bytes
// are cut into Data packets (id, resend until ACKed), the camera's Data is
// reordered, ACKed every 10 ms with a selective map and the measured receive
// rate, and delivered in order. It implements io.ReadWriteCloser and is meant
// to be the transport of a bc.Client.
type Conn struct {
	sock    *sock
	ownSock bool
	peer    netip.AddrPort
	cid     int32 // ours, on what the camera sends
	did     int32 // the camera's, on what we send
	cfg     ConnConfig

	mu   sync.Mutex
	cond *sync.Cond

	closed bool
	err    error

	// sending
	nextID uint32
	sent   map[uint32][]byte // encoded packets without ACK

	// receiving
	want    uint32            // next packet to deliver
	recvd   map[uint32][]byte // arrived ahead of want
	gapFrom time.Time         // since when want is missing while later ones are here
	rx      [][]byte          // delivered, not yet read
	rxBytes int

	lastRx time.Time

	// receive rate reported to the camera: payload bytes of the last second,
	// latched once per second
	rateFrom  time.Time
	rateBytes uint64
	rate      uint32

	stats Stats

	done chan struct{}
}

// NewConn builds a Conn on an existing socket whose peer already agreed on the
// ids (the handshake is Discover's job). It takes over the socket.
func NewConn(pc *net.UDPConn, peer netip.AddrPort, cid, did int32, cfg ConnConfig) *Conn {
	return newConn(newSock(pc), true, peer, cid, did, cfg)
}

func newConn(s *sock, own bool, peer netip.AddrPort, cid, did int32, cfg ConnConfig) *Conn {
	cfg.setDefaults()
	now := time.Now()
	c := &Conn{
		sock: s, ownSock: own,
		peer: netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port()),
		cid:  cid, did: did, cfg: cfg,
		sent:     map[uint32][]byte{},
		recvd:    map[uint32][]byte{},
		lastRx:   now,
		rateFrom: now,
		done:     make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)

	s.setHandler(c.onPacket)
	rebound := c.sendHB // tell the peer that the client is the same after a rebind
	s.rebound.Store(&rebound)

	go c.run()
	return c
}

func (c *Conn) LocalAddr() net.Addr { return c.sock.conn().LocalAddr() }

func (c *Conn) RemoteAddr() net.Addr { return net.UDPAddrFromAddrPort(c.peer) }

// Stats returns a snapshot of the counters.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.RecvRate, s.PacketsWant, s.Unacked = c.rate, c.want, len(c.sent)
	s.Rebinds = c.sock.rebinds.Load()
	return s
}

// Read returns stream bytes in order. After an error or Close it still returns
// what was already delivered, then the error.
func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.rx) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.rx) == 0 {
		return 0, c.err
	}

	var n int
	for n < len(p) && len(c.rx) > 0 {
		k := copy(p[n:], c.rx[0])
		n += k
		c.rxBytes -= k
		if k == len(c.rx[0]) {
			c.rx[0] = nil
			c.rx = c.rx[1:]
		} else {
			c.rx[0] = c.rx[0][k:]
		}
	}
	return n, nil
}

// Write cuts b into Data packets and sends them. It blocks while the camera
// has too many packets unacknowledged.
func (c *Conn) Write(b []byte) (int, error) {
	for off := 0; off < len(b); {
		chunk := b[off:min(off+MaxPayload, len(b))]

		wire, err := c.reserve(chunk)
		if err != nil {
			return off, err
		}
		_ = c.sock.send(wire, c.peer) // lost packets are sent again
		off += len(chunk)
	}
	return len(b), nil
}

// reserve gives the chunk its packet id and keeps it until it is ACKed.
func (c *Conn) reserve(chunk []byte) ([]byte, error) {
	deadline := time.Now().Add(c.cfg.WriteStall)

	for {
		c.mu.Lock()
		if c.closed {
			err := c.err
			c.mu.Unlock()
			return nil, err
		}
		if len(c.sent) < c.cfg.MaxUnacked {
			defer c.mu.Unlock()
			id := c.nextID
			c.nextID++
			wire, _ := (&Data{ConnID: c.did, PacketID: id, Payload: chunk}).Marshal()
			c.sent[id] = wire
			return wire, nil
		}
		c.mu.Unlock()

		if time.Now().After(deadline) {
			return nil, ErrWindow
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Close ends the connection: a C2D_DISC to the camera (best effort), the
// goroutines stop, a socket that came from Discover is closed. Idempotent.
func (c *Conn) Close() error {
	c.finish(ErrClosed, true)
	return nil
}

func (c *Conn) finish(err error, disc bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed, c.err = true, err
	c.cond.Broadcast()
	c.mu.Unlock()

	close(c.done)
	if disc {
		wire := c.discovery(&C2dDisc{CID: c.cid, DID: c.did})
		_ = c.sock.send(wire, c.peer)
	}
	if c.ownSock {
		_ = c.sock.Close()
	}
}

func (c *Conn) discovery(m Message) []byte {
	b, _ := (&Discovery{TID: uint32(rand.IntN(255) + 1), XML: m}).Marshal()
	return b
}

func (c *Conn) sendHB() {
	_ = c.sock.send(c.discovery(&C2dHb{CID: c.cid, DID: c.did}), c.peer)
}

// onPacket runs on the socket's read goroutine.
func (c *Conn) onPacket(b []byte, from netip.AddrPort) {
	if from != c.peer {
		return
	}
	p, err := Decode(b)
	if err != nil {
		return
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.lastRx = time.Now()

	switch p := p.(type) {
	case *Discovery:
		switch m := p.XML.(type) {
		case *D2cHb:
			c.mu.Unlock()
			if m.CID != c.cid || m.DID != c.did {
				c.cfg.Log.Warnf("camera heartbeat has other ids (cid %d did %d)", m.CID, m.DID)
			}
			c.sendHB() // answer, the camera checks the client lives
			return
		case *D2cDisc:
			c.mu.Unlock()
			c.finish(ErrPeerClosed, false)
			return
		case *R2cDisc:
			c.mu.Unlock()
			c.finish(ErrRelayEnded, false)
			return
		}

	case *Ack:
		if p.ConnID == c.cid {
			c.handleAck(p)
		}

	case *Data:
		if p.ConnID == c.cid {
			c.handleData(p)
		}
	}
	c.mu.Unlock()
}

// handleAck drops what the camera has: everything up to PacketID and what the
// selective map marks as received.
func (c *Conn) handleAck(a *Ack) {
	if a.PacketID == NoAck {
		return
	}
	for id := range c.sent {
		if id <= a.PacketID {
			delete(c.sent, id)
		}
	}
	for i, got := range a.Payload {
		if got > 0 {
			delete(c.sent, a.PacketID+1+uint32(i))
		}
	}
}

func (c *Conn) handleData(d *Data) {
	c.rateBytes += uint64(len(d.Payload))

	if d.PacketID < c.want {
		return // already delivered, a resend that crossed our ACK
	}
	if d.PacketID > c.want {
		c.stats.Reordered++
		if d.PacketID-c.want > 1<<14 {
			return // far beyond any window, not worth the memory
		}
	}
	if _, dup := c.recvd[d.PacketID]; !dup {
		c.recvd[d.PacketID] = d.Payload
	}
	c.deliver()
}

// deliver moves the contiguous packets to the read queue.
func (c *Conn) deliver() {
	var moved bool
	for {
		payload, ok := c.recvd[c.want]
		if !ok {
			break
		}
		delete(c.recvd, c.want)
		c.want++
		c.rx = append(c.rx, payload)
		c.rxBytes += len(payload)
		moved = true
	}

	if moved {
		c.gapFrom = time.Time{}
		// nobody reads: drop the oldest, the framing above resyncs
		for c.rxBytes > c.cfg.RxLimit && len(c.rx) > 1 {
			c.rxBytes -= len(c.rx[0])
			c.stats.Dropped += len(c.rx[0])
			c.rx = c.rx[1:]
		}
		c.cond.Broadcast()
	}
	if len(c.recvd) > 0 && c.gapFrom.IsZero() {
		c.gapFrom = time.Now()
	}
}

// skipGap gives up on missing packets once they were due for too long: the
// stream gets a hole (one damaged message above) and carries on.
func (c *Conn) skipGap(now time.Time) {
	if len(c.recvd) == 0 || c.gapFrom.IsZero() || now.Sub(c.gapFrom) < c.cfg.GapSkip {
		return
	}
	next := slices.Min(mapKeys(c.recvd))
	if next > c.want {
		c.cfg.Log.Warnf("lost packets %d..%d skipped after %s", c.want, next-1, now.Sub(c.gapFrom).Round(time.Millisecond))
		c.want = next
		c.stats.GapSkips++
	}
	c.gapFrom = time.Time{}
	c.deliver()
}

func mapKeys(m map[uint32][]byte) []uint32 {
	keys := make([]uint32, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// buildAck is the ACK for the current state: the last packet in order, the
// map of what came after it, and the measured receive rate.
func (c *Conn) buildAck() *Ack {
	if c.want == 0 {
		return EmptyAck(c.did)
	}

	a := &Ack{ConnID: c.did, PacketID: c.want - 1, RecvBytesPerSec: c.rate}
	if len(c.recvd) > 0 {
		last := slices.Max(mapKeys(c.recvd))
		n := min(int(last-c.want)+1, maxSACK)
		a.Payload = make([]byte, n)
		for i := range a.Payload {
			if _, ok := c.recvd[c.want+uint32(i)]; ok {
				a.Payload[i] = 1
			}
		}
	}
	return a
}

// latchRate fixes the receive rate for the next second of ACKs. The camera's
// congestion control takes it as its bandwidth estimate: 0 caps the stream at
// about 2 Mbit/s, a constant throttles it, the real rate lets it ramp up.
func (c *Conn) latchRate(now time.Time) {
	elapsed := now.Sub(c.rateFrom)
	if elapsed < time.Second {
		return
	}
	rate := float64(c.rateBytes) / elapsed.Seconds()
	c.rate = uint32(min(rate, float64(^uint32(0))))
	c.rateBytes, c.rateFrom = 0, now
}

// run drives everything on a clock: ACKs, resends, heartbeat, gap skipping,
// the receive rate and the silence timeout.
func (c *Conn) run() {
	tick := time.NewTicker(c.cfg.AckInterval)
	defer tick.Stop()

	lastResend, lastHB := time.Now(), time.Now()
	c.sendHB() // the camera expects them from the start

	for {
		select {
		case <-c.done:
			return
		case now := <-tick.C:
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			if now.Sub(c.lastRx) > c.cfg.Timeout {
				c.mu.Unlock()
				c.finish(ErrTimeout, false)
				return
			}

			c.latchRate(now)
			c.skipGap(now)
			ack := c.buildAck()

			var resend [][]byte
			if now.Sub(lastResend) >= c.cfg.ResendInterval {
				lastResend = now
				ids := mapKeys(c.sent)
				slices.Sort(ids)
				for _, id := range ids {
					resend = append(resend, c.sent[id])
				}
				c.stats.Resends += len(resend)
			}
			c.mu.Unlock()

			wire, _ := ack.Marshal()
			_ = c.sock.send(wire, c.peer)
			for _, w := range resend {
				_ = c.sock.send(w, c.peer)
			}

			if now.Sub(lastHB) >= c.cfg.HBInterval {
				lastHB = now
				c.sendHB()
			}
		}
	}
}
