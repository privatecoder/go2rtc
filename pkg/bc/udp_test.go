package bc

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/bc/udp"
	"github.com/stretchr/testify/require"
)

// lanCamera answers the LAN query for a UID on a socket and then talks the
// Baichuan protocol over the reliable UDP transport, using the same Conn as
// the client with the ids swapped.
func lanCamera(t *testing.T, f *fakeCamera) netip.AddrPort {
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	addr := pc.LocalAddr().(*net.UDPAddr).AddrPort()

	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			p, err := udp.Decode(buf[:n])
			if err != nil {
				continue
			}
			d, ok := p.(*udp.Discovery)
			if !ok {
				continue
			}
			q, ok := d.XML.(*udp.C2dC)
			if !ok {
				continue
			}

			const did = 4711
			reply, _ := (&udp.Discovery{TID: d.TID, XML: &udp.D2cCr{CID: q.CID, DID: did}}).Marshal()
			_, _ = pc.WriteToUDPAddrPort(reply, from)

			// from here on the Conn owns the socket (no reader of ours any more);
			// resent queries are ignored by it
			cam := udp.NewConn(pc, from, did, q.CID, udp.ConnConfig{})
			f.serveConn(cam)
			return
		}
	}()
	return addr
}

func TestDialUIDOverUDP(t *testing.T) {
	f := newBareFake(t)
	f.stream = swan(t)
	f.talkAbility = adpcmAbility(8000, 400)
	cam := lanCamera(t, f)

	prod, err := dial("bc://admin:123456@95270000ABCDEFGH?stream=main&discovery=local", func(o *udp.Options) {
		o.Broadcasts = []netip.AddrPort{cam}
		o.ReplyTimeout = 3 * time.Second
		o.Log = udp.Logger{Debugf: t.Logf, Infof: t.Logf, Warnf: t.Logf}
	})
	require.NoError(t, err)
	p := prod.(*Producer)
	t.Cleanup(func() { p.Stop() })

	require.Equal(t, "udp", p.Protocol)
	require.Equal(t, cam.String(), p.RemoteAddr)
	require.NotNil(t, p.video)
	require.Equal(t, "H264", p.videoCodec)
	require.NotNil(t, p.audio)
	require.Len(t, p.GetMedias(), 3, "video, audio and the talk backchannel")

	// the same login, keepalive and stream code ran over UDP
	f.mu.Lock()
	require.Equal(t, []uint16{0xdc12}, f.loginCodes)
	f.mu.Unlock()

	// frames flow, and the camera's keepalive is answered over the UDP transport
	var v packets
	p.video.Input = v.add
	errc := make(chan error, 1)
	go func() { errc <- p.Start() }()
	require.Eventually(t, func() bool { return len(v.all()) == 3 }, 5*time.Second, 5*time.Millisecond)

	f.keepAlive(555)
	select {
	case m := <-f.replies:
		require.Equal(t, uint16(555), m.MsgNum)
	case <-time.After(2 * time.Second):
		t.Fatal("no keepalive reply over UDP")
	}

	require.NoError(t, p.Stop())
	<-errc
}

func TestDialUIDNoCamera(t *testing.T) {
	// nothing answers: the error is reported, not a hang
	silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer silent.Close()

	start := time.Now()
	_, err = dial("bc://admin:123456@95270000ABCDEFGH?discovery=local", func(o *udp.Options) {
		o.Broadcasts = []netip.AddrPort{silent.LocalAddr().(*net.UDPAddr).AddrPort()}
		o.ReplyTimeout = 300 * time.Millisecond
	})
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
}
