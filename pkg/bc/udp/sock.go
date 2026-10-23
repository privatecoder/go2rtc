package udp

import (
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

const (
	portMin, portMax = 53500, 54000 // the range the official client uses
	sockBuffer       = 4 * 1024 * 1024
	sendStall        = 250 * time.Millisecond
)

// sock is the UDP socket of one session: first the discovery handshake reads
// it, then the Conn takes over. Packets are handed to the current handler on
// the read goroutine. A write that fails or stalls rebinds the socket on the
// same port, like neolink does for lossy mobile links.
type sock struct {
	mu     sync.RWMutex
	pc     *net.UDPConn
	port   int
	closed atomic.Bool

	handler  atomic.Pointer[func([]byte, netip.AddrPort)]
	rebound  atomic.Pointer[func()]
	rebinds  atomic.Int64
	lastBind time.Time

	done chan struct{}
}

// listenSock binds a random port of the official range with big buffers.
func listenSock() (*sock, error) {
	ports := rand.Perm(portMax - portMin)
	var lastErr error
	for _, p := range ports {
		pc, err := net.ListenUDP("udp4", &net.UDPAddr{Port: portMin + p})
		if err == nil {
			return newSock(pc), nil
		}
		lastErr = err
	}
	// the range is busy, any port does, the register server learns it from us
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		return nil, lastErr
	}
	return newSock(pc), nil
}

func newSock(pc *net.UDPConn) *sock {
	_ = pc.SetReadBuffer(sockBuffer)
	_ = pc.SetWriteBuffer(sockBuffer)
	s := &sock{pc: pc, port: pc.LocalAddr().(*net.UDPAddr).Port, done: make(chan struct{})}
	go s.readLoop()
	return s
}

func (s *sock) conn() *net.UDPConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pc
}

func (s *sock) Port() int { return s.port }

func (s *sock) setHandler(h func([]byte, netip.AddrPort)) { s.handler.Store(&h) }

func (s *sock) readLoop() {
	defer close(s.done)

	buf := make([]byte, 64*1024)
	var failures int
	for {
		pc := s.conn()
		n, from, err := pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			if s.closed.Load() {
				return
			}
			if s.conn() != pc {
				continue // rebound meanwhile
			}
			// ICMP errors and the like are not the end of the socket
			if failures++; failures > 1000 {
				return
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		failures = 0

		if h := s.handler.Load(); h != nil {
			(*h)(buf[:n], netip.AddrPortFrom(from.Addr().Unmap(), from.Port()))
		}
	}
}

// send writes a datagram. A stall over 250 ms or an error rebinds the socket.
func (s *sock) send(b []byte, to netip.AddrPort) error {
	pc := s.conn()
	_ = pc.SetWriteDeadline(time.Now().Add(sendStall))
	_, err := pc.WriteToUDPAddrPort(b, to)
	if err != nil && !s.closed.Load() {
		s.rebind(pc)
	}
	return err
}

// rebind replaces a socket that failed, keeping the local port (the register
// server and the camera know it). The old socket is closed first, binds are
// exclusive. If the bind fails the dead socket stays and the next failed send
// tries again.
func (s *sock) rebind(old *net.UDPConn) {
	s.mu.Lock()
	if s.closed.Load() || s.pc != old || time.Since(s.lastBind) < sendStall {
		s.mu.Unlock()
		return
	}
	s.lastBind = time.Now()
	_ = old.Close()

	pc, err := net.ListenUDP("udp4", &net.UDPAddr{Port: s.port})
	if err != nil {
		s.mu.Unlock()
		return
	}
	_ = pc.SetReadBuffer(sockBuffer)
	_ = pc.SetWriteBuffer(sockBuffer)
	s.pc = pc
	s.rebinds.Add(1)
	s.mu.Unlock()

	if f := s.rebound.Load(); f != nil {
		(*f)()
	}
}

func (s *sock) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	// not waiting for the read goroutine, Close is also called from its handler
	return s.conn().Close()
}
