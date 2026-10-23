package udp

import (
	"bytes"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testCID = 82000
	testDID = 80
)

// fakeCam is the camera end of a Conn: a raw socket that records what the
// client sends and sends whatever a test wants.
type fakeCam struct {
	t    *testing.T
	pc   *net.UDPConn
	conn *Conn
	to   netip.AddrPort // the client's socket

	mu    sync.Mutex
	acks  []*Ack
	datas []*Data
	discs []Message
	from  []netip.AddrPort
}

func fastCfg() ConnConfig {
	return ConnConfig{
		GapSkip: 100 * time.Millisecond, Timeout: 5 * time.Second, ResendInterval: 80 * time.Millisecond,
		AckInterval: 5 * time.Millisecond, HBInterval: 50 * time.Millisecond,
	}
}

func newPair(t *testing.T, cfg ConnConfig) (*Conn, *fakeCam) {
	return newPairIDs(t, cfg, testCID, testDID)
}

func newPairIDs(t *testing.T, cfg ConnConfig, cid, did int32) (*Conn, *fakeCam) {
	camPC, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	cliPC, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	cam := &fakeCam{t: t, pc: camPC, to: cliPC.LocalAddr().(*net.UDPAddr).AddrPort()}
	cam.conn = NewConn(cliPC, camPC.LocalAddr().(*net.UDPAddr).AddrPort(), cid, did, cfg)
	go cam.readLoop()
	t.Cleanup(func() { cam.conn.Close(); camPC.Close() })
	return cam.conn, cam
}

func (f *fakeCam) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, from, err := f.pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			return
		}
		p, err := Decode(buf[:n])
		if err != nil {
			continue
		}
		f.mu.Lock()
		f.from = append(f.from, from)
		switch p := p.(type) {
		case *Ack:
			f.acks = append(f.acks, p)
		case *Data:
			f.datas = append(f.datas, p)
		case *Discovery:
			f.discs = append(f.discs, p.XML)
		}
		f.mu.Unlock()
	}
}

func (f *fakeCam) send(p Packet) {
	b, err := p.Marshal()
	require.NoError(f.t, err)
	_, err = f.pc.WriteToUDPAddrPort(b, f.to)
	require.NoError(f.t, err)
}

func (f *fakeCam) data(id uint32, payload []byte) {
	f.send(&Data{ConnID: testCID, PacketID: id, Payload: payload})
}

func (f *fakeCam) lastAck() *Ack {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.acks) == 0 {
		return nil
	}
	return f.acks[len(f.acks)-1]
}

func (f *fakeCam) dataIDs() []uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []uint32
	for _, d := range f.datas {
		ids = append(ids, d.PacketID)
	}
	return ids
}

func (f *fakeCam) countDiscs(name string) (n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.discs {
		if m.XMLName() == name {
			n++
		}
	}
	return
}

func readN(t *testing.T, c *Conn, n int) []byte {
	t.Helper()
	got := make([]byte, 0, n)
	buf := make([]byte, 4096)
	deadline := time.AfterFunc(5*time.Second, func() { c.Close() })
	defer deadline.Stop()
	for len(got) < n {
		k, err := c.Read(buf)
		require.NoError(t, err)
		got = append(got, buf[:k]...)
	}
	return got
}

func TestConnGoldenSamples(t *testing.T) {
	// the two Data packets of a captured legacy login: 1330 + 526 bytes, one BC message
	c, cam := newPairIDs(t, fastCfg(), 80, testCID)
	for _, name := range []string{"udp_multi_3.bin", "udp_multi_4.bin"} {
		_, err := cam.pc.WriteToUDPAddrPort(sample(t, name), cam.to)
		require.NoError(t, err)
	}
	got := readN(t, c, 1856)
	require.Equal(t, []byte{0xf0, 0xde, 0xbc, 0x0a}, got[:4], "BC magic")
}

func TestConnWriteChunks(t *testing.T) {
	c, cam := newPair(t, fastCfg())
	msg := make([]byte, 5000)
	for i := range msg {
		msg[i] = byte(i)
	}
	n, err := c.Write(msg)
	require.NoError(t, err)
	require.Equal(t, len(msg), n)

	require.Eventually(t, func() bool { return len(cam.dataIDs()) >= 4 }, time.Second, time.Millisecond)
	cam.mu.Lock()
	defer cam.mu.Unlock()
	var got []byte
	for i, d := range cam.datas[:4] {
		require.Equal(t, int32(testDID), d.ConnID, "sent to the camera's id")
		require.Equal(t, uint32(i), d.PacketID)
		if i < 3 {
			require.Len(t, d.Payload, 1330, "MTU 1350 minus the 20 byte header")
		}
		got = append(got, d.Payload...)
	}
	require.Equal(t, msg, got)

	// 1330*3 + 1010
	require.Len(t, cam.datas[3].Payload, 1010)

	// empty write sends nothing
	_, err = c.Write(nil)
	require.NoError(t, err)
}

func TestConnResendUntilAcked(t *testing.T) {
	c, cam := newPair(t, fastCfg())
	_, err := c.Write(bytes.Repeat([]byte{1}, 3000)) // 3 packets
	require.NoError(t, err)

	// no ACK: all three come again
	require.Eventually(t, func() bool { return len(cam.dataIDs()) >= 9 }, 2*time.Second, 5*time.Millisecond)
	require.GreaterOrEqual(t, c.Stats().Resends, 6)

	// ACK 0 and a selective map for 2 (1 is still missing)
	cam.send(&Ack{ConnID: testCID, PacketID: 0, Payload: []byte{0, 1}})
	require.Eventually(t, func() bool { return c.Stats().Unacked == 1 }, time.Second, time.Millisecond)

	before := len(cam.dataIDs())
	time.Sleep(300 * time.Millisecond)
	cam.mu.Lock()
	for _, d := range cam.datas[before:] {
		require.Equal(t, uint32(1), d.PacketID, "only the missing packet is sent again")
	}
	cam.mu.Unlock()

	cam.send(&Ack{ConnID: testCID, PacketID: 2})
	require.Eventually(t, func() bool { return c.Stats().Unacked == 0 }, time.Second, time.Millisecond)

	// acks of other connections and the empty ack change nothing
	_, err = c.Write([]byte("x"))
	require.NoError(t, err)
	cam.send(&Ack{ConnID: 999, PacketID: 3})
	cam.send(EmptyAck(testCID))
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, c.Stats().Unacked)
}

func TestConnWindowBlocksWrite(t *testing.T) {
	cfg := fastCfg()
	cfg.MaxUnacked = 4
	cfg.WriteStall = 200 * time.Millisecond
	c, cam := newPair(t, cfg)

	_, err := c.Write(make([]byte, 4*MaxPayload))
	require.NoError(t, err)

	// the fifth packet waits for window space; an ACK frees it
	done := make(chan error, 1)
	go func() { _, err := c.Write([]byte("more")); done <- err }()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("write did not wait")
	default:
	}
	cam.send(&Ack{ConnID: testCID, PacketID: 1})
	require.NoError(t, <-done)

	// and without ACKs it gives up
	_, err = c.Write(make([]byte, 10*MaxPayload))
	require.ErrorIs(t, err, ErrWindow)
}

func TestConnReceiveInOrder(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	// before anything arrives the ack says so
	require.Eventually(t, func() bool { return cam.lastAck() != nil }, time.Second, time.Millisecond)
	require.Equal(t, EmptyAck(testDID), cam.lastAck())

	cam.data(0, []byte("hello "))
	cam.data(1, []byte("world"))
	require.Equal(t, "hello world", string(readN(t, c, 11)))

	require.Eventually(t, func() bool { a := cam.lastAck(); return a.PacketID == 1 }, time.Second, time.Millisecond)
	a := cam.lastAck()
	require.Equal(t, int32(testDID), a.ConnID, "acks carry the camera's id")
	require.Equal(t, uint32(0), a.GroupID)
	require.Empty(t, a.Payload)

	// duplicates and old packets are ignored
	cam.data(0, []byte("hello "))
	cam.data(1, []byte("XXXXX"))
	cam.data(2, []byte("!"))
	require.Equal(t, "!", string(readN(t, c, 1)))
}

func TestConnReorderAndSelectiveAck(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	cam.data(0, []byte("a"))
	require.Equal(t, "a", string(readN(t, c, 1)))

	// 1 is late, 2 and 3 are here
	cam.data(2, []byte("c"))
	cam.data(3, []byte("d"))
	require.Eventually(t, func() bool {
		a := cam.lastAck()
		return a != nil && a.PacketID == 0 && len(a.Payload) == 3
	}, time.Second, time.Millisecond)
	require.Equal(t, []byte{0, 1, 1}, cam.lastAck().Payload, "missing, received, received after packet 0")

	cam.data(1, []byte("b"))
	require.Equal(t, "bcd", string(readN(t, c, 3)))
	require.Eventually(t, func() bool { a := cam.lastAck(); return a.PacketID == 3 && len(a.Payload) == 0 }, time.Second, time.Millisecond)
	require.Equal(t, 2, c.Stats().Reordered)
	require.Zero(t, c.Stats().GapSkips)
}

func TestConnLargeShuffledStream(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	// a keyframe worth of packets in a shuffled order inside a window of 8
	const packets = 300
	want := make([]byte, 0, packets*MaxPayload)
	payloads := make([][]byte, packets)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i)}, MaxPayload)
		want = append(want, payloads[i]...)
	}
	// a permutation inside blocks of 8
	for blk := 0; blk < packets; blk += 8 {
		perm := rand.Perm(min(8, packets-blk))
		for _, p := range perm {
			cam.data(uint32(blk+p), payloads[blk+p])
		}
	}
	require.True(t, bytes.Equal(want, readN(t, c, len(want))))
	require.Zero(t, c.Stats().GapSkips)
}

func TestConnGapSkip(t *testing.T) {
	cfg := fastCfg()
	cfg.GapSkip = 120 * time.Millisecond
	c, cam := newPair(t, cfg)

	cam.data(0, []byte("a"))
	require.Equal(t, "a", string(readN(t, c, 1)))

	cam.data(2, []byte("c")) // 1 never comes
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, c.Stats().GapSkips, "not before the wait is over")

	require.Equal(t, "c", string(readN(t, c, 1)))
	require.Equal(t, 1, c.Stats().GapSkips)
	require.Eventually(t, func() bool { return cam.lastAck().PacketID == 2 }, time.Second, time.Millisecond)

	// the wait runs from when the gap appeared, not from the last delivery:
	// after a long idle time a late packet is not skipped at once
	time.Sleep(300 * time.Millisecond)
	cam.data(4, []byte("e"))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, c.Stats().GapSkips)
	cam.data(3, []byte("d"))
	require.Equal(t, "de", string(readN(t, c, 2)))
	require.Equal(t, 1, c.Stats().GapSkips)
}

func TestConnReceiveRateIsMeasured(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	// 120 packets of 1000 bytes inside the first second
	for i := 0; i < 120; i++ {
		cam.data(uint32(i), bytes.Repeat([]byte{1}, 1000))
		if i%20 == 19 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	readN(t, c, 120_000)

	// until the second is over the camera hears 0
	require.Zero(t, cam.lastAck().RecvBytesPerSec)

	// then the bytes per second of that window, held until the next latch
	require.Eventually(t, func() bool { return cam.lastAck().RecvBytesPerSec > 0 }, 2*time.Second, 5*time.Millisecond)
	rate := cam.lastAck().RecvBytesPerSec
	require.InDelta(t, 120_000, rate, 6_000, "bytes per second, not per 100 ms")
	require.Equal(t, rate, c.Stats().RecvRate)

	// idle second: the next window had nothing
	require.Eventually(t, func() bool { return cam.lastAck().RecvBytesPerSec == 0 }, 2500*time.Millisecond, 10*time.Millisecond)

	// a steady 400 packets/s stream is reported at its real rate
	stop := make(chan struct{})
	go func() {
		id := uint32(120)
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				cam.data(id, bytes.Repeat([]byte{1}, 1000))
				id++
			}
		}
	}()
	defer close(stop)
	go io.Copy(io.Discard, c)
	require.Eventually(t, func() bool {
		r := cam.lastAck().RecvBytesPerSec
		return r > 150_000 && r < 210_000 // 200 packets/s * 1000 B, timer slack
	}, 4*time.Second, 20*time.Millisecond)
}

func TestConnHeartbeats(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	// C2D_HB from the start and then every interval
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_HB") >= 4 }, 2*time.Second, 5*time.Millisecond)
	cam.mu.Lock()
	hb := cam.discs[0].(*C2dHb)
	cam.mu.Unlock()
	require.Equal(t, &C2dHb{CID: testCID, DID: testDID}, hb)

	// the camera's heartbeat is answered at once
	before := cam.countDiscs("C2D_HB")
	cam.send(&Discovery{TID: 3, XML: &D2cHb{CID: testCID, DID: testDID}})
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_HB") > before }, 30*time.Millisecond+20*time.Millisecond, time.Millisecond)
	_ = c
}

func TestConnTimeout(t *testing.T) {
	cfg := fastCfg()
	cfg.Timeout = 250 * time.Millisecond
	c, cam := newPair(t, cfg)

	// any packet from the camera is life
	for i := 0; i < 4; i++ {
		cam.send(&Discovery{TID: 3, XML: &D2cHb{CID: testCID, DID: testDID}})
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-c.done:
		t.Fatal("closed while the camera was alive")
	default:
	}

	errc := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); errc <- err }()
	select {
	case err := <-errc:
		require.ErrorIs(t, err, ErrTimeout)
	case <-time.After(2 * time.Second):
		t.Fatal("silence did not end the connection")
	}
	_, err := c.Write([]byte("x"))
	require.ErrorIs(t, err, ErrTimeout)
}

func TestConnPeerTerminate(t *testing.T) {
	c, cam := newPair(t, fastCfg())
	cam.data(0, []byte("last words"))
	cam.send(&Discovery{TID: 9, XML: &D2cDisc{CID: testCID, DID: testDID}})

	// what was delivered can still be read, then the error
	require.Equal(t, "last words", string(readN(t, c, 10)))
	_, err := c.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrPeerClosed)

	c2, cam2 := newPair(t, fastCfg())
	cam2.send(&Discovery{TID: 9, XML: &R2cDisc{SID: 1}})
	_, err = c2.Read(make([]byte, 1))
	require.ErrorIs(t, err, ErrRelayEnded)
}

func TestConnClose(t *testing.T) {
	c, cam := newPair(t, fastCfg())
	errc := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); errc <- err }()
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close()) // idempotent
	require.ErrorIs(t, <-errc, ErrClosed)

	// the camera is told
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_DISC") == 1 }, time.Second, time.Millisecond)
	cam.mu.Lock()
	var disc *C2dDisc
	for _, m := range cam.discs {
		if d, ok := m.(*C2dDisc); ok {
			disc = d
		}
	}
	cam.mu.Unlock()
	require.Equal(t, &C2dDisc{CID: testCID, DID: testDID}, disc)

	_, err := c.Write([]byte("x"))
	require.ErrorIs(t, err, ErrClosed)
}

func TestConnIgnoresStrangers(t *testing.T) {
	c, cam := newPair(t, fastCfg())

	// another socket, same ids
	other, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer other.Close()
	b, _ := (&Data{ConnID: testCID, PacketID: 0, Payload: []byte("evil")}).Marshal()
	_, err = other.WriteToUDPAddrPort(b, cam.to)
	require.NoError(t, err)

	// right peer, wrong connection id, and garbage
	cam.send(&Data{ConnID: 5, PacketID: 0, Payload: []byte("wrong id")})
	_, err = cam.pc.WriteToUDPAddrPort([]byte("garbage"), cam.to)
	require.NoError(t, err)
	d, _ := (&Discovery{TID: 1, XML: &C2dHb{}}).Marshal()
	d[len(d)-1] ^= 0xff // bad checksum
	_, err = cam.pc.WriteToUDPAddrPort(d, cam.to)
	require.NoError(t, err)

	cam.data(0, []byte("good"))
	require.Equal(t, "good", string(readN(t, c, 4)))
}

func TestConnRxLimitDropsOldest(t *testing.T) {
	cfg := fastCfg()
	cfg.RxLimit = 5000
	c, cam := newPair(t, cfg)

	for i := 0; i < 10; i++ {
		cam.data(uint32(i), bytes.Repeat([]byte{byte('a' + i)}, 1000))
	}
	require.Eventually(t, func() bool { return c.Stats().PacketsWant == 10 }, time.Second, time.Millisecond)
	require.Equal(t, 5000, c.Stats().Dropped)

	got := readN(t, c, 5000)
	require.Equal(t, byte('f'), got[0], "the oldest were dropped, the newest kept")
	require.Equal(t, byte('j'), got[4999])
}

func TestConnSocketRebind(t *testing.T) {
	c, cam := newPair(t, fastCfg())
	cam.data(0, []byte("a"))
	readN(t, c, 1)
	hbBefore := cam.countDiscs("C2D_HB")
	port := c.sock.Port()

	// the socket dies under the connection (what a network change does)
	_ = c.sock.conn().Close()

	require.Eventually(t, func() bool { return c.Stats().Rebinds >= 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, port, c.sock.conn().LocalAddr().(*net.UDPAddr).Port, "same local port")

	// and it works again, both ways
	cam.data(1, []byte("b"))
	require.Equal(t, "b", string(readN(t, c, 1)))
	_, err := c.Write([]byte("hello"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(cam.dataIDs()) > 0 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return cam.countDiscs("C2D_HB") > hbBefore }, time.Second, time.Millisecond)
}

// lossyProxy sits between two sockets and drops some datagrams.
func lossyProxy(t *testing.T, loss float64, a, b *net.UDPConn, aAddr, bAddr netip.AddrPort) {
	forward := func(from, to *net.UDPConn, dest netip.AddrPort) {
		buf := make([]byte, 65536)
		for {
			n, _, err := from.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			if rand.Float64() < loss {
				continue
			}
			_, _ = to.WriteToUDPAddrPort(buf[:n], dest)
		}
	}
	go forward(a, b, bAddr)
	go forward(b, a, aAddr)
}

func TestConnBothEndsOverLossyLink(t *testing.T) {
	// two Conns talk through a proxy that drops 10 % of the datagrams: resends and
	// the selective ACK must deliver every byte, in order, in both directions
	cliPC, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	camPC, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	p1, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}) // the camera sees us here
	p2, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}) // we see the camera here
	ap := func(c *net.UDPConn) netip.AddrPort { return c.LocalAddr().(*net.UDPAddr).AddrPort() }
	lossyProxy(t, 0.10, p2, p1, ap(cliPC), ap(camPC))
	t.Cleanup(func() { p1.Close(); p2.Close() })

	cfg := fastCfg()
	cfg.ResendInterval = 40 * time.Millisecond
	cfg.GapSkip = time.Minute // never skip, loss is repaired
	cfg.Timeout = 20 * time.Second
	cli := NewConn(cliPC, ap(p2), 11, 22, cfg)
	cam := NewConn(camPC, ap(p1), 22, 11, cfg)
	t.Cleanup(func() { cli.Close(); cam.Close() })

	up := make([]byte, 250_000)
	down := make([]byte, 400_000)
	for i := range up {
		up[i] = byte(i * 7)
	}
	for i := range down {
		down[i] = byte(i * 13)
	}

	go func() { cli.Write(up) }()
	go func() { cam.Write(down) }()

	var wg sync.WaitGroup
	check := func(c *Conn, want []byte) {
		defer wg.Done()
		got := readN(t, c, len(want))
		require.True(t, bytes.Equal(want, got))
	}
	wg.Add(2)
	go check(cam, up)
	go check(cli, down)
	wg.Wait()

	require.Greater(t, cli.Stats().Resends+cam.Stats().Resends, 0, "the proxy really dropped packets")
	require.Zero(t, cli.Stats().GapSkips+cam.Stats().GapSkips)
}
