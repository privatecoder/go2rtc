package bc

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/bc/udp"
)

const DefaultPort = "9000"

var (
	ErrClosed       = errors.New("bc: client closed")
	ErrTimeout      = errors.New("bc: timeout waiting for camera reply")
	ErrLoginFailed  = errors.New("bc: login failed, check username and password")
	ErrPingTimeout  = errors.New("bc: camera stopped answering pings")
	ErrWriteTimeout = errors.New("bc: write to the camera timed out")
	ErrWriteQueue   = errors.New("bc: outbound queue full, camera does not read")
	errPingUnsupp   = errors.New("bc: camera does not support pings")
)

// StreamKind selects one of the camera's encoder streams.
type StreamKind uint8

const (
	StreamMain   StreamKind = iota
	StreamSub               // SD
	StreamExtern            // the hidden "Balanced" stream, same as sub on cameras without it
)

func ParseStreamKind(s string) (StreamKind, bool) {
	switch s {
	case "", "main":
		return StreamMain, true
	case "sub":
		return StreamSub, true
	case "extern":
		return StreamExtern, true
	}
	return 0, false
}

// params are the three distinct values the official client sets per stream:
// header stream_type, Preview handle and Preview streamType.
func (k StreamKind) params() (streamType uint8, handle uint32, name string) {
	switch k {
	case StreamSub:
		return 1, 256, "subStream"
	case StreamExtern:
		return 0, 1024, "externStream"
	}
	return 0, 0, "mainStream"
}

// MaxEncryption is the highest encryption level the client offers at login,
// the camera may pick a lower one.
type MaxEncryption uint8

const (
	EncryptionDefault MaxEncryption = iota // AES, like neolink's login()
	EncryptionNone
	EncryptionBC
	EncryptionAES
)

func ParseMaxEncryption(s string) (MaxEncryption, bool) {
	switch s {
	case "":
		return EncryptionDefault, true
	case "none":
		return EncryptionNone, true
	case "bc":
		return EncryptionBC, true
	case "aes":
		return EncryptionAES, true
	}
	return 0, false
}

// loginCode is the response_code of the legacy login upgrade request.
func (e MaxEncryption) loginCode() uint16 {
	switch e {
	case EncryptionNone:
		return 0xdc00
	case EncryptionBC:
		return 0xdc01
	}
	return 0xdc12
}

// Config of a Client, zero values get defaults.
type Config struct {
	Username, Password string
	Channel            uint8

	// Encryption is the maximum encryption requested at login, AES if unset.
	Encryption MaxEncryption

	ReplyTimeout   time.Duration // wait for a reply, default 10 s
	WriteTimeout   time.Duration // one write to the camera, default 10 s
	PingInterval   time.Duration // default 5 s
	PingTimeout    time.Duration // default 5 s
	MaxMissedPings int           // consecutive, default 5
	StreamBuffer   int           // frames buffered per stream, default 128
	WriteQueue     int           // outbound messages queued for the writer, default 128

	// Name prefixes every log message (the UID or host of the camera).
	Name string
	// Log receives the diagnostics, default is the package level Log.
	Log Logger
}

// Logger has a function per level: Debugf for routine steps, Infof for
// outcomes (logged in, the chosen path), Warnf for trouble (drops, retries,
// format changes). Nil functions discard.
type Logger = udp.Logger

// Log is the default logger, the application replaces it to use its own.
var Log = Logger{
	Debugf: func(format string, args ...any) {},
	Infof:  func(format string, args ...any) { log.Printf("[bc] "+format, args...) },
	Warnf:  func(format string, args ...any) { log.Printf("[bc] warning: "+format, args...) },
}

func (c *Config) setDefaults() {
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.WriteQueue == 0 {
		c.WriteQueue = 128
	}
	if c.ReplyTimeout == 0 {
		c.ReplyTimeout = 10 * time.Second
	}
	if c.PingInterval == 0 {
		c.PingInterval = 5 * time.Second
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = 5 * time.Second
	}
	if c.MaxMissedPings == 0 {
		c.MaxMissedPings = 5
	}
	if c.StreamBuffer == 0 {
		c.StreamBuffer = 128
	}
	if c.Log.Debugf == nil && c.Log.Infof == nil && c.Log.Warnf == nil {
		c.Log = Log
	}
	c.Log = c.Log.Filled()
	if c.Name != "" {
		c.Log = c.Log.Prefix(c.Name)
	}
}

// Client is one logged-in control connection. A single goroutine reads the
// transport and dispatches: camera keepalives are answered in place, replies
// go to waiters by msg_num and media goes to streams through bounded,
// non-blocking queues, so a slow consumer can never stall keepalives.
type Client struct {
	cfg   Config
	conn  io.ReadWriteCloser
	codec *Codec

	wmu  sync.Mutex // encode and enqueue in one order
	out  chan outItem
	prio chan outItem // keepalive replies, written first
	num  atomic.Uint32

	mu      sync.Mutex
	subs    map[uint16]*sub
	streams map[uint16]*Stream
	talk    *Talk
	closed  bool
	err     error

	done chan struct{}
	wg   sync.WaitGroup

	// DeviceInfo from the login reply, nil before Login
	Info *DeviceInfo
}

// DialTCP connects to host[:port], default port 9000, with a 5 s timeout.
func DialTCP(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", tcpAddr(addr), 5*time.Second)
}

// tcpAddr adds the default port. The host may be a bracketed IPv6 literal
// (as in a URL) or a bare one.
func tcpAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	host := strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
	return net.JoinHostPort(host, DefaultPort)
}

// NewClient starts the reader and writer goroutines on conn. Call Login next.
func NewClient(conn io.ReadWriteCloser, cfg Config) *Client {
	cfg.setDefaults()
	c := &Client{
		cfg:     cfg,
		conn:    conn,
		codec:   NewCodec(cfg.Password),
		out:     make(chan outItem, cfg.WriteQueue),
		prio:    make(chan outItem, 8),
		subs:    map[uint16]*sub{},
		streams: map[uint16]*Stream{},
		done:    make(chan struct{}),
	}
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	return c
}

// Done is closed when the client is closed or failed.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err is why the client stopped, nil while it runs.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close is idempotent. It closes the transport, unblocks waiters and closes
// every stream channel, and waits (briefly) for the goroutines.
func (c *Client) Close() error {
	c.fail(ErrClosed)
	finished := make(chan struct{})
	go func() { c.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
	}
	return nil
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.err = err
	close(c.done)
	c.mu.Unlock()
	_ = c.conn.Close()
}

// outItem is one encoded message for the writer, or a flush marker.
type outItem struct {
	b       []byte
	flush   chan struct{}
	written chan struct{} // closed when the datagram was written, if asked for
}

// freeNumLocked takes the next msg_num that nothing uses: not an active
// stream, a waiter, or a number still in binary mode in the codec.
func (c *Client) freeNumLocked() uint16 {
	for {
		n := uint16(c.num.Add(1))
		if n == 0 || c.subs[n] != nil || c.streams[n] != nil || c.codec.isBinary(n) {
			continue
		}
		return n
	}
}

func (c *Client) freeNum() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.freeNumLocked()
}

// send encodes m and queues it for the writer; it never blocks. A full queue
// means the camera does not read and closes the client.
func (c *Client) send(m *Message) error { return c.enqueue(c.out, m) }

func (c *Client) enqueue(q chan outItem, m *Message) error {
	return c.enqueueNotify(q, m, nil)
}

// sendNotify is send with a channel that is closed once the writer has
// written the message. If the client fails first it is never closed.
func (c *Client) sendNotify(m *Message) (<-chan struct{}, error) {
	written := make(chan struct{})
	if err := c.enqueueNotify(c.out, m, written); err != nil {
		return nil, err
	}
	return written, nil
}

func (c *Client) enqueueNotify(q chan outItem, m *Message, written chan struct{}) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	select {
	case <-c.done:
		return c.Err()
	default:
	}

	b, err := c.codec.Encode(m)
	if err != nil {
		return err
	}
	select {
	case q <- outItem{b: b, written: written}:
		return nil
	default:
		c.fail(ErrWriteQueue)
		return ErrWriteQueue
	}
}

// Flush waits until everything queued so far has been written, or the
// timeout, or the client stops.
func (c *Client) Flush(timeout time.Duration) {
	flushed := make(chan struct{})
	select {
	case c.out <- outItem{flush: flushed}:
	default:
		return
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-flushed:
	case <-t.C:
	case <-c.done:
	}
}

// writeLoop is the only goroutine that writes to the transport. A write that
// takes longer than the write timeout closes the client, which also unblocks
// transports without deadlines.
func (c *Client) writeLoop() {
	defer c.wg.Done()

	for {
		var it outItem
		select {
		case it = <-c.prio:
		default:
			select {
			case it = <-c.prio:
			case it = <-c.out:
			case <-c.done:
				return
			}
		}

		if it.flush != nil {
			close(it.flush)
			continue
		}

		timer := time.AfterFunc(c.cfg.WriteTimeout, func() { c.fail(ErrWriteTimeout) })
		_, err := c.conn.Write(it.b)
		timer.Stop()
		if err != nil {
			c.fail(err)
			return
		}
		if it.written != nil {
			close(it.written)
		}
	}
}

// sub receives the replies with one msg_num and, if set, one msg_id.
type sub struct {
	c   *Client
	num uint16
	id  uint32
	ch  chan *Message
}

// subscribeNew allocates a free msg_num and registers the waiter atomically.
func (c *Client) subscribeNew(id uint32) (*sub, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, c.errLocked()
	}
	s := &sub{c: c, num: c.freeNumLocked(), id: id, ch: make(chan *Message, 16)}
	c.subs[s.num] = s
	return s, nil
}

// subscribe registers a waiter for a number the caller owns (a running stream).
func (c *Client) subscribe(num uint16, id uint32) (*sub, error) {
	s := &sub{c: c, num: num, id: id, ch: make(chan *Message, 16)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, c.errLocked()
	}
	c.subs[num] = s
	return s, nil
}

func (c *Client) errLocked() error {
	if c.err != nil {
		return c.err
	}
	return ErrClosed
}

func (s *sub) close() {
	s.c.mu.Lock()
	if s.c.subs[s.num] == s {
		delete(s.c.subs, s.num)
	}
	s.c.mu.Unlock()
}

func (s *sub) recv(timeout time.Duration) (*Message, error) {
	return s.recvCancel(timeout, nil)
}

// ErrCanceled is returned when a wait is cut short by its cancel channel.
var ErrCanceled = errors.New("bc: canceled")

// recvCancel waits for a reply, the timeout, the client closing or cancel.
func (s *sub) recvCancel(timeout time.Duration, cancel <-chan struct{}) (*Message, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case m := <-s.ch:
		return m, nil
	case <-t.C:
		return nil, ErrTimeout
	case <-s.c.done:
		return nil, s.c.Err()
	case <-cancel:
		return nil, ErrCanceled
	}
}

func (c *Client) readLoop() {
	defer c.wg.Done()
	defer c.closeStreams()

	rd := NewReader(c.conn, c.codec)
	for {
		m, err := rd.ReadMessage()
		if err != nil {
			c.fail(err)
			return
		}
		c.dispatch(m)
	}
}

func (c *Client) closeStreams() {
	c.mu.Lock()
	streams := c.streams
	c.streams = map[uint16]*Stream{}
	c.mu.Unlock()
	for _, s := range streams {
		s.closeC()
	}
}

func (c *Client) dispatch(m *Message) {
	if m.MsgID == MsgUDPKeepAlive {
		// the camera checks we are alive, reply with the same msg_num; the
		// reply goes to the priority queue, the reader never waits for a write
		_ = c.enqueue(c.prio, &Message{
			MsgID: MsgUDPKeepAlive, ChannelID: m.ChannelID, StreamType: m.StreamType,
			MsgNum: m.MsgNum, ResponseCode: 200, Class: ClassModern24,
		})
		return
	}

	c.mu.Lock()
	st, sb := c.streams[m.MsgNum], c.subs[m.MsgNum]
	c.mu.Unlock()

	if st != nil && m.Binary != nil && m.MsgID == MsgVideo {
		st.feed(m.Binary)
	}
	if sb != nil && (sb.id == 0 || sb.id == m.MsgID) {
		select {
		case sb.ch <- m:
		default: // nobody is reading, e.g. binary after the start reply
		}
	}
}

// Login runs the neolink login flow: legacy header-only upgrade request, the
// camera answers with the encryption mode and a nonce, then the modern
// LoginUser/LoginNet with MD5(user+nonce) / MD5(pass+nonce).
func (c *Client) Login() (*DeviceInfo, error) {
	sb, err := c.subscribeNew(MsgLogin)
	if err != nil {
		return nil, err
	}
	defer sb.close()
	num := sb.num

	err = c.send(&Message{
		MsgID: MsgLogin, ChannelID: c.cfg.Channel, MsgNum: num,
		ResponseCode: c.cfg.Encryption.loginCode(), Class: ClassLegacy, LegacyUpgrade: true,
	})
	if err != nil {
		return nil, err
	}

	r, err := sb.recv(c.cfg.ReplyTimeout)
	if err != nil {
		return nil, fmt.Errorf("bc: login: %w", err)
	}
	if r.Body == nil || r.Body.Encryption == nil {
		return nil, fmt.Errorf("bc: login: expected encryption reply, got code %d", r.ResponseCode)
	}
	nonce := r.Body.Encryption.Nonce

	err = c.send(&Message{
		MsgID: MsgLogin, ChannelID: c.cfg.Channel, MsgNum: num, Class: ClassModern24,
		Body: &Body{
			LoginUser: &LoginUser{
				Version:  XMLVersion,
				UserName: ModernHash(c.cfg.Username + nonce),
				Password: ModernHash(c.cfg.Password + nonce),
				UserVer:  1,
			},
			LoginNet: &LoginNet{Version: XMLVersion, Type: "LAN"},
		},
	})
	if err != nil {
		return nil, err
	}

	r, err = sb.recv(c.cfg.ReplyTimeout)
	if err != nil {
		return nil, fmt.Errorf("bc: login: %w", err)
	}
	if r.ResponseCode != 200 {
		return nil, ErrLoginFailed
	}
	if r.Body == nil || r.Body.DeviceInfo == nil {
		if r.Extension == nil {
			return nil, ErrLoginFailed
		}
		return nil, errors.New("bc: login: expected DeviceInfo reply")
	}

	c.Info = r.Body.DeviceInfo
	c.cfg.Log.Infof("logged in: %s", c.Info.describe())
	c.wg.Add(1)
	go c.pingLoop()
	return c.Info, nil
}

// pingLoop mirrors neolink: ping (msg 93, the reply carries LinkType) every
// interval. A reply that is not a LinkType, or any non-200 reply, means the
// camera has no ping support: pinging stops for good (logged once) and the
// connection stays. Too many consecutive timeouts close the client.
func (c *Client) pingLoop() {
	defer c.wg.Done()

	t := time.NewTicker(c.cfg.PingInterval)
	defer t.Stop()

	var missed int
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}

		switch err := c.ping(); {
		case err == nil:
			missed = 0
		case errors.Is(err, errPingUnsupp):
			c.cfg.Log.Warnf("pings not supported by the camera (%v), keeping the connection without them", err)
			return
		case errors.Is(err, ErrTimeout):
			if missed++; missed >= c.cfg.MaxMissedPings {
				c.fail(ErrPingTimeout)
				return
			}
		default:
			c.fail(err)
			return
		}
	}
}

func (c *Client) ping() error {
	sb, err := c.subscribeNew(MsgPing)
	if err != nil {
		return err
	}
	defer sb.close()

	err = c.send(&Message{MsgID: MsgPing, ChannelID: c.cfg.Channel, MsgNum: sb.num, Class: ClassModern24})
	if err != nil {
		return err
	}
	r, err := sb.recv(c.cfg.PingTimeout)
	if err != nil {
		return err
	}
	if r.ResponseCode != 200 {
		return fmt.Errorf("%w: code %d", errPingUnsupp, r.ResponseCode)
	}
	if r.Body == nil || r.Body.LinkType == nil {
		return fmt.Errorf("%w: no LinkType in the reply", errPingUnsupp)
	}
	return nil
}

// GetTalkAbility asks which audio formats the camera accepts for talk.
func (c *Client) GetTalkAbility() (*TalkAbility, error) {
	return c.getTalkAbility(c.cfg.ReplyTimeout)
}

func (c *Client) getTalkAbility(timeout time.Duration) (*TalkAbility, error) {
	sb, err := c.subscribeNew(MsgTalkAbility)
	if err != nil {
		return nil, err
	}
	defer sb.close()

	ext := NewExtension()
	ext.ChannelID = &c.cfg.Channel
	err = c.send(&Message{MsgID: MsgTalkAbility, ChannelID: c.cfg.Channel, MsgNum: sb.num, Class: ClassModern24, Extension: ext})
	if err != nil {
		return nil, err
	}
	r, err := sb.recv(timeout)
	if err != nil {
		return nil, err
	}
	if r.Body == nil || r.Body.TalkAbility == nil {
		return nil, fmt.Errorf("bc: no TalkAbility in reply (code %d)", r.ResponseCode)
	}
	return r.Body.TalkAbility, nil
}

// Stream is a running video stream. Frames arrive on C, which is closed when
// the stream or client stops.
//
// Every frame is stamped by the reader goroutine before the queue decides
// whether to drop it (Media.PTS, AudioPos), so dropped frames still advance
// the clocks and a consumer sees the real gaps.
type Stream struct {
	C chan *Media

	c    *Client
	num  uint16
	kind StreamKind

	mu     sync.Mutex // guards closed and sends on C
	closed bool

	// reader goroutine only
	dm       Demuxer
	vclock   videoClock
	audio    audioClock
	waitKey  bool
	lastWarn time.Time
	warnDrop uint64

	dropped atomic.Uint64
	stop    sync.Once
}

// StartStream requests a stream (msg 3) and waits for the camera to accept.
func (c *Client) StartStream(kind StreamKind) (*Stream, error) {
	streamType, handle, name := kind.params()
	st := &Stream{C: make(chan *Media, c.cfg.StreamBuffer), c: c, kind: kind}

	// number, stream and waiter are registered in one step
	c.mu.Lock()
	if c.closed {
		err := c.errLocked()
		c.mu.Unlock()
		return nil, err
	}
	st.num = c.freeNumLocked()
	sb := &sub{c: c, num: st.num, id: MsgVideo, ch: make(chan *Message, 16)}
	c.streams[st.num] = st
	c.subs[st.num] = sb
	c.mu.Unlock()
	defer sb.close()

	err := c.send(&Message{
		MsgID: MsgVideo, ChannelID: c.cfg.Channel, StreamType: streamType, MsgNum: st.num, Class: ClassModern24,
		Body: &Body{Preview: &Preview{Version: XMLVersion, ChannelID: c.cfg.Channel, Handle: handle, StreamType: name}},
	})
	if err == nil {
		var r *Message
		if r, err = sb.recv(c.cfg.ReplyTimeout); err == nil && r.ResponseCode != 200 {
			err = fmt.Errorf("bc: camera refused the %s, code %d", name, r.ResponseCode)
		}
	}
	if err != nil {
		st.unregister()
		st.closeC()
		c.codec.clearBinary(st.num)
		return nil, err
	}
	return st, nil
}

func (s *Stream) unregister() {
	s.c.mu.Lock()
	if s.c.streams[s.num] == s {
		delete(s.c.streams, s.num)
	}
	s.c.mu.Unlock()
}

func (s *Stream) closeC() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.C)
	}
	s.mu.Unlock()
}

// Dropped is the number of frames dropped because the consumer was too slow.
func (s *Stream) Dropped() uint64 { return s.dropped.Load() }

// feed runs on the reader goroutine.
func (s *Stream) feed(b []byte) {
	s.dm.Push(b)
	for m := s.dm.Next(); m != nil; m = s.dm.Next() {
		s.stamp(m)
		s.deliver(m)
	}
}

// stamp assigns the frame its place on the stream's clocks.
func (s *Stream) stamp(m *Media) {
	switch m.Kind {
	case KindIframe, KindPframe:
		m.PTS = s.vclock.next(m.Microseconds)

	case KindAAC:
		if info, ok := parseADTS(m.Data); ok {
			m.AudioRate = info.rate
			s.stampAudio(m, info.rate, info.samples)
		}

	case KindADPCM:
		if n := len(m.Data) - adpcmHeaderLen; n > 0 {
			m.AudioRate = adpcmSampleRate
			s.stampAudio(m, adpcmSampleRate, uint64(2*n))
		}
	}
}

func (s *Stream) stampAudio(m *Media, rate uint32, samples uint64) {
	pos, re := s.audio.next(rate, samples, s.vclock.acc, s.vclock.started)
	m.AudioPos = pos
	if re {
		s.c.cfg.Log.Debugf("audio clock put back on the video clock (frames were lost)")
	}
}

// deliver never blocks. When the consumer is behind, the frame is dropped
// and, if it was video, video is held back until the next keyframe so the
// decoder never sees a P-frame without its reference.
func (s *Stream) deliver(m *Media) {
	switch m.Kind {
	case KindPframe:
		if s.waitKey {
			s.noteDrop()
			return
		}
	case KindIframe:
		s.waitKey = false
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	select {
	case s.C <- m:
		s.mu.Unlock()
		return
	default:
	}
	s.mu.Unlock()

	if m.Kind == KindIframe || m.Kind == KindPframe {
		s.waitKey = true
	}
	s.noteDrop()
}

// noteDrop counts a drop and logs it now and then. It runs without any lock,
// a slow logger must not hold up anything else.
func (s *Stream) noteDrop() {
	s.dropped.Add(1)
	s.warnDrop++
	if now := time.Now(); now.Sub(s.lastWarn) >= 5*time.Second {
		n := s.warnDrop
		s.lastWarn, s.warnDrop = now, 0
		s.c.cfg.Log.Warnf("slow consumer, dropped %d frame(s) to keep the connection alive", n)
	}
}

// Stop ends the stream (msg 4). With wait > 0 it waits that long for the
// camera's acknowledgement, otherwise the request is only queued.
func (s *Stream) Stop(wait time.Duration) error {
	var err error
	s.stop.Do(func() {
		s.unregister()
		s.closeC()
		defer s.c.codec.clearBinary(s.num)

		c := s.c
		select {
		case <-c.done:
			return
		default:
		}

		var sb *sub
		if sb, err = c.subscribe(s.num, MsgVideoStop); err != nil {
			return
		}
		defer sb.close()

		streamType, handle, _ := s.kind.params()
		err = c.send(&Message{
			MsgID: MsgVideoStop, ChannelID: c.cfg.Channel, StreamType: streamType, MsgNum: s.num, Class: ClassModern24,
			Body: &Body{Preview: &Preview{Version: XMLVersion, ChannelID: c.cfg.Channel, Handle: handle}},
		})
		if err != nil || wait <= 0 {
			return
		}
		var r *Message
		if r, err = sb.recv(wait); err != nil {
			if errors.Is(err, ErrTimeout) {
				err = nil // the camera did not bother to answer
			}
			return
		}
		if r.ResponseCode != 200 {
			err = fmt.Errorf("bc: stop stream: camera replied %d", r.ResponseCode)
		}
	})
	return err
}
