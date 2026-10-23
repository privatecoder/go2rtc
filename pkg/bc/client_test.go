package bc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoginAndStream(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)

	c := dialFake(t, f, fastConfig())
	info, err := c.Login()
	require.NoError(t, err)
	require.Equal(t, uint32(2560), info.Resolution.Width)
	// AES was negotiated from the nonce and password
	require.Equal(t, ModeAES, c.codec.Cipher().Mode)
	require.Equal(t, AESKey(fakeNonce, fakePass), c.codec.Cipher().Key)

	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	var want Demuxer
	want.Push(f.stream)
	wantFrames := drain(&want)

	var got []*Media
	for len(got) < len(wantFrames) {
		select {
		case m := <-st.C:
			require.NotNil(t, m)
			got = append(got, m)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d of %d frames", len(got), len(wantFrames))
		}
	}
	// the stream stamps the frames, otherwise they are what the demuxer gives
	for _, m := range got {
		if m.Kind == KindADPCM {
			require.Equal(t, uint32(8000), m.AudioRate)
		}
		m.PTS, m.AudioPos, m.AudioRate = 0, 0, 0
	}
	require.Equal(t, wantFrames, got)
	require.Zero(t, st.Dropped())

	// stop is acknowledged and the channel closes
	require.NoError(t, st.Stop(time.Second))
	require.NoError(t, st.Stop(time.Second)) // idempotent
	_, ok := <-st.C
	require.False(t, ok)
	f.mu.Lock()
	require.Equal(t, []Preview{{Version: "1.1", Handle: 0, StreamType: ""}}, f.stops)
	require.Equal(t, []Preview{{Version: "1.1", Handle: 0, StreamType: "mainStream"}}, f.previews)
	f.mu.Unlock()
}

func TestStreamKinds(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	for _, kind := range []StreamKind{StreamMain, StreamSub, StreamExtern} {
		st, err := c.StartStream(kind)
		require.NoError(t, err)
		require.NoError(t, st.Stop(time.Second))
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// neolink stream.rs: main 0/0, sub 1/256, extern 0/1024 (header stream_type / handle)
	require.Equal(t, []Preview{
		{Version: "1.1", Handle: 0, StreamType: "mainStream"},
		{Version: "1.1", Handle: 256, StreamType: "subStream"},
		{Version: "1.1", Handle: 1024, StreamType: "externStream"},
	}, f.previews)
	require.Equal(t, []uint8{0, 1, 0}, f.types)
}

func TestLoginWrongPassword(t *testing.T) {
	f := newFakeCamera(t)
	cfg := fastConfig()
	cfg.Password = "wrong"
	c := dialFake(t, f, cfg)

	_, err := c.Login()
	require.ErrorIs(t, err, ErrLoginFailed)
}

func TestKeepAliveReply(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	f.keepAlive(777)
	select {
	case m := <-f.replies:
		require.Equal(t, uint16(777), m.MsgNum)
		require.Equal(t, uint16(200), m.ResponseCode)
	case <-time.After(2 * time.Second):
		t.Fatal("no keepalive reply")
	}
}

func TestKeepAliveWithSlowConsumer(t *testing.T) {
	f := newFakeCamera(t)
	f.stream = swan(t)
	f.repeat = 40 // ~12 MB, nobody reads the stream channel

	cfg := fastConfig()
	cfg.StreamBuffer = 8
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	// wait for the flood to fill the queue and overflow it
	require.Eventually(t, func() bool { return st.Dropped() > 50 }, 5*time.Second, 5*time.Millisecond)

	// the reader goroutine is still alive and answers right away
	f.keepAlive(4242)
	select {
	case m := <-f.replies:
		require.Equal(t, uint16(4242), m.MsgNum)
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive stalled behind a slow consumer")
	}

	// after a drop video resumes at a keyframe, never at a P-frame
	for len(st.C) > 0 {
		<-st.C
	}
	require.Eventually(t, func() bool {
		select {
		case m := <-st.C:
			if m.Kind == KindIframe {
				return true
			}
			require.NotEqual(t, KindPframe, m.Kind, "P-frame without a keyframe after drop")
		default:
		}
		return false
	}, 5*time.Second, time.Millisecond)
}

func TestPingTimeout(t *testing.T) {
	f := newFakeCamera(t)
	f.ping = pingIgnore

	cfg := fastConfig()
	cfg.PingInterval = 10 * time.Millisecond
	cfg.PingTimeout = 10 * time.Millisecond
	cfg.MaxMissedPings = 3
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("client did not give up on a silent camera")
	}
	require.ErrorIs(t, c.Err(), ErrPingTimeout)
	require.Eventually(t, func() bool { return f.pingCount() >= 3 }, time.Second, time.Millisecond)

	// everything is unblocked
	_, ok := <-st.C
	require.False(t, ok)
	_, err = c.StartStream(StreamMain)
	require.ErrorIs(t, err, ErrPingTimeout)
	require.NoError(t, c.Close())
}

func TestPingAnsweredKeepsAlive(t *testing.T) {
	f := newFakeCamera(t)

	cfg := fastConfig()
	cfg.PingInterval = 10 * time.Millisecond
	cfg.PingTimeout = 100 * time.Millisecond
	cfg.MaxMissedPings = 2
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	require.Eventually(t, func() bool { return f.pingCount() >= 5 }, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, c.Err())
}

func TestPingUnsupportedStopsPinging(t *testing.T) {
	f := newFakeCamera(t)
	f.ping = pingEmpty

	cfg := fastConfig()
	cfg.PingInterval = 10 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.MaxMissedPings = 1
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	require.NoError(t, c.Err(), "a camera without ping support must stay connected")
	require.Equal(t, 1, f.pingCount())
}

func TestPingRejectedStopsPinging(t *testing.T) {
	f := newFakeCamera(t)
	f.ping = pingReject

	var logs atomic.Int32
	cfg := fastConfig()
	cfg.Logf = func(string, ...any) { logs.Add(1) }
	cfg.PingInterval = 10 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	c := dialFake(t, f, cfg)
	_, err := c.Login()
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)
	require.NoError(t, c.Err(), "a camera that rejects pings must stay connected")
	require.Equal(t, 1, f.pingCount())
	require.Equal(t, int32(1), logs.Load(), "logged once")
}

func TestCloseUnblocksEverything(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)
	st, err := c.StartStream(StreamMain)
	require.NoError(t, err)

	// a waiter parked on a reply nobody will send
	errc := make(chan error, 1)
	go func() {
		sb, err := c.subscribeNew(0)
		if err == nil {
			_, err = sb.recv(time.Minute)
		}
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close()) // idempotent

	select {
	case err := <-errc:
		require.ErrorIs(t, err, ErrClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("waiter still blocked")
	}
	_, ok := <-st.C
	require.False(t, ok)
	require.ErrorIs(t, c.Err(), ErrClosed)
	require.NoError(t, st.Stop(0))
}

func TestStartStreamConnectionLost(t *testing.T) {
	f := newFakeCamera(t)
	c := dialFake(t, f, fastConfig())
	_, err := c.Login()
	require.NoError(t, err)

	f.mu.Lock()
	_ = f.conn.Close()
	f.mu.Unlock()
	_, err = c.StartStream(StreamMain)
	require.Error(t, err)
}
