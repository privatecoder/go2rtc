package bc

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	fakeUser  = "admin"
	fakePass  = "123456"
	fakeNonce = "9E6D1FCB9E69846D"
)

type pingMode int

const (
	pingLinkType pingMode = iota // answer like a normal camera
	pingIgnore                   // never answer
	pingEmpty                    // 200 without LinkType, "pings not supported"
	pingReject                   // 400
)

// fakeCamera is an in-process camera: it speaks the real wire format with
// AES negotiated, using the package's own codec on its side.
type fakeCamera struct {
	t  *testing.T
	ln net.Listener

	ping pingMode

	// talk: ability sent when set, else the command is refused with 400
	talkAbility *TalkAbility
	talkFail    int // answer this many TalkConfigs with 422 before accepting

	// talk session model: with talkExclusive the camera has one session between
	// an accepted config and the reset, a config while one is open gets 422 and
	// a block outside a session is recorded as a violation
	talkExclusive bool
	talkActive    bool
	violations    []string
	configDelay   time.Duration // the answers come late
	resetDelay    time.Duration
	ackCode       uint16 // answer to blocks, 200 if 0
	eventAt       []time.Time
	stream        []byte // BcMedia bytes sent after a video start
	repeat        int    // how many times, default 1

	mu       sync.Mutex
	conn     io.ReadWriteCloser
	codec    *Codec
	wmu      sync.Mutex
	pings    int
	previews []Preview
	types    []uint8 // header stream_type of each video start
	stops    []Preview
	replies  chan *Message // client messages with msg id 234
	started  chan struct{}

	loginCodes []uint16        // response codes of the legacy login requests
	videoReq   *Message        // the stream start request, media goes out under its msg_num
	binSent    bool            // the first media message carried the binary-mode extension
	talkEvents []string        // "ability", "config:422", "config:200", "reset", in order
	talkConfig []TalkConfig    // every config received
	talkBlocks []talkBlock     // every msg 202 payload
	talkNums   map[uint16]bool // msg_nums used by msg 202
}

type talkBlock struct {
	at   time.Time
	num  uint16
	data []byte
}

// newBareFake is a fake camera without a transport, serveConn runs it on one.
func newBareFake(t *testing.T) *fakeCamera {
	f := &fakeCamera{
		t: t, repeat: 1,
		codec:   NewCodec(fakePass),
		replies: make(chan *Message, 16),
		started: make(chan struct{}, 4),
	}
	t.Cleanup(func() {
		if f.ln != nil {
			_ = f.ln.Close()
		}
		f.mu.Lock()
		if f.conn != nil {
			_ = f.conn.Close()
		}
		f.mu.Unlock()
	})
	return f
}

func newFakeCamera(t *testing.T) *fakeCamera {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	f := newBareFake(t)
	f.ln = ln
	go f.serve()
	return f
}

func (f *fakeCamera) addr() string { return f.ln.Addr().String() }

func (f *fakeCamera) pingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pings
}

func (f *fakeCamera) write(m *Message) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	b, err := f.codec.Encode(m)
	if err != nil {
		f.t.Errorf("fake camera encode: %v", err)
		return
	}
	f.conn.Write(b) // the client may be gone by now
}

// keepAlive is what the camera sends to check the client.
func (f *fakeCamera) keepAlive(num uint16) {
	f.write(&Message{MsgID: MsgUDPKeepAlive, MsgNum: num, Class: ClassModern24})
}

func reply(req *Message, code uint16) *Message {
	return &Message{
		MsgID: req.MsgID, ChannelID: req.ChannelID, StreamType: req.StreamType,
		MsgNum: req.MsgNum, ResponseCode: code, Class: ClassModern,
	}
}

func (f *fakeCamera) serve() {
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	f.serveConn(conn)
}

func (f *fakeCamera) serveConn(conn io.ReadWriteCloser) {
	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()

	rd := NewReader(conn, f.codec)
	for {
		m, err := rd.ReadMessage()
		if err != nil {
			return
		}

		switch m.MsgID {
		case MsgLogin:
			if m.Class == ClassLegacy {
				// encryption negotiation: the best the client offers, the
				// camera supports everything. The reply announces it with BC
				// encryption (plain for "none").
				f.mu.Lock()
				f.loginCodes = append(f.loginCodes, m.ResponseCode)
				f.mu.Unlock()

				var code uint16
				var announce, after Cipher
				switch m.ResponseCode {
				case 0xdc00:
					code, announce, after = 0xdd00, Cipher{Mode: ModeNone}, Cipher{Mode: ModeNone}
				case 0xdc01:
					code, announce, after = 0xdd01, Cipher{Mode: ModeBC}, Cipher{Mode: ModeBC}
				default:
					code, announce, after = 0xdd02, Cipher{Mode: ModeBC}, NewAESCipher(ModeAES, fakeNonce, fakePass)
					require.Equal(f.t, uint16(0xdc12), m.ResponseCode)
				}
				r := &Message{
					MsgID: MsgLogin, MsgNum: m.MsgNum, ResponseCode: code, Class: ClassModern20,
					Body: &Body{Encryption: &Encryption{Version: XMLVersion, Type: "md5", Nonce: fakeNonce}},
				}
				b, err := encodeMessage(r, announce)
				require.NoError(f.t, err)
				f.wmu.Lock()
				conn.Write(b)
				f.wmu.Unlock()
				f.codec.SetCipher(after)
				continue
			}

			u := m.Body.LoginUser
			if u.UserName != ModernHash(fakeUser+fakeNonce) || u.Password != ModernHash(fakePass+fakeNonce) {
				f.write(reply(m, 400))
				continue
			}
			r := reply(m, 200)
			r.Body = &Body{DeviceInfo: &DeviceInfo{
				Version: XMLVersion, Resolution: &Resolution{Name: "2560*1440", Width: 2560, Height: 1440},
			}}
			f.write(r)

		case MsgPing:
			f.mu.Lock()
			f.pings++
			f.mu.Unlock()
			switch f.ping {
			case pingLinkType:
				r := reply(m, 200)
				r.Body = &Body{LinkType: &LinkType{Type: "LAN"}}
				f.write(r)
			case pingEmpty:
				f.write(reply(m, 200))
			case pingReject:
				f.write(reply(m, 400))
			}

		case MsgUDPKeepAlive:
			f.replies <- m

		case MsgVideo:
			f.mu.Lock()
			f.previews = append(f.previews, *m.Body.Preview)
			f.types = append(f.types, m.StreamType)
			f.mu.Unlock()
			f.write(reply(m, 200))
			go f.sendMedia(m)

		case MsgTalkAbility:
			f.mu.Lock()
			f.talkEvents = append(f.talkEvents, "ability")
			f.eventAt = append(f.eventAt, time.Now())
			ability := f.talkAbility
			f.mu.Unlock()
			if ability == nil {
				f.write(reply(m, 400))
				continue
			}
			r := reply(m, 200)
			r.Body = &Body{TalkAbility: ability}
			f.write(r)

		case MsgTalkConfig:
			f.mu.Lock()
			f.talkConfig = append(f.talkConfig, *m.Body.TalkConfig)
			code := uint16(200)
			if f.talkFail > 0 {
				f.talkFail--
				code = 422
			} else if f.talkExclusive && f.talkActive {
				code = 422
			}
			if code == 200 {
				f.talkActive = true
			}
			f.talkEvents = append(f.talkEvents, fmt.Sprintf("config:%d", code))
			f.eventAt = append(f.eventAt, time.Now())
			delay := f.configDelay
			f.mu.Unlock()
			f.later(delay, reply(m, code))

		case MsgTalkReset:
			f.mu.Lock()
			f.talkActive = false
			f.talkEvents = append(f.talkEvents, "reset")
			f.eventAt = append(f.eventAt, time.Now())
			delay := f.resetDelay
			f.mu.Unlock()
			f.later(delay, reply(m, 200))

		case MsgTalk:
			f.mu.Lock()
			if f.talkExclusive && !f.talkActive {
				f.violations = append(f.violations, "block outside a session")
			}
			f.talkBlocks = append(f.talkBlocks, talkBlock{at: time.Now(), num: m.MsgNum, data: m.Binary})
			f.talkEvents = append(f.talkEvents, "block")
			f.eventAt = append(f.eventAt, time.Now())
			if f.talkNums == nil {
				f.talkNums = map[uint16]bool{}
			}
			f.talkNums[m.MsgNum] = true
			code := f.ackCode
			f.mu.Unlock()
			if code == 0 {
				code = 200
			}
			f.write(reply(m, code))

		case MsgVideoStop:
			f.mu.Lock()
			f.stops = append(f.stops, *m.Body.Preview)
			f.mu.Unlock()
			f.write(reply(m, 200))
		}
	}
}

// sendMedia streams the BcMedia bytes in odd sized binary messages.
func (f *fakeCamera) sendMedia(req *Message) {
	const chunk = 12345
	f.mu.Lock()
	f.videoReq = req
	f.mu.Unlock()
	f.started <- struct{}{}
	for i := 0; i < f.repeat; i++ {
		for off := 0; off < len(f.stream); off += chunk {
			f.push(f.stream[off:min(off+chunk, len(f.stream))])
		}
	}
}

// push sends more media on the running stream; the first message announces
// binary mode in its extension, like the real camera.
func (f *fakeCamera) push(data []byte) {
	f.mu.Lock()
	req, first := f.videoReq, !f.binSent
	f.binSent = true
	f.mu.Unlock()

	m := reply(req, 200)
	m.Binary = data
	if first {
		m.Extension = NewExtension()
		m.Extension.BinaryData = new(uint32)
		*m.Extension.BinaryData = 1
	}
	f.write(m)
}

func fastConfig() Config {
	return Config{
		Username: fakeUser, Password: fakePass,
		ReplyTimeout: 2 * time.Second,
		Log:          Logger{},
	}
}

func dialFake(t *testing.T, f *fakeCamera, cfg Config) *Client {
	conn, err := DialTCP(f.addr())
	require.NoError(t, err)
	c := NewClient(conn, cfg)
	t.Cleanup(func() { c.Close() })
	return c
}

func adpcmAbility(rate, lengthPerEncoder uint16) *TalkAbility {
	return &TalkAbility{
		Version:             XMLVersion,
		DuplexList:          []DuplexList{{Duplex: "FDX"}},
		AudioStreamModeList: []AudioStreamModeList{{AudioStreamMode: []string{"followVideoStream"}}},
		AudioConfigList: []AudioConfigList{{AudioConfig: AudioConfig{
			AudioType: "adpcm", SampleRate: rate, SamplePrecision: 16,
			LengthPerEncoder: lengthPerEncoder, SoundTrack: "mono",
		}}},
	}
}

func (f *fakeCamera) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.talkEvents...)
}

func (f *fakeCamera) blocks() []talkBlock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]talkBlock(nil), f.talkBlocks...)
}

// sessionEvents is events() without the ability query and the blocks.
func (f *fakeCamera) sessionEvents() []string {
	var out []string
	for _, e := range f.events() {
		if e != "ability" && e != "block" {
			out = append(out, e)
		}
	}
	return out
}

// later answers after a delay without holding up the camera.
func (f *fakeCamera) later(d time.Duration, m *Message) {
	if d == 0 {
		f.write(m)
		return
	}
	time.AfterFunc(d, func() { f.write(m) })
}

// eventTime is when the n-th event of that name (counting from 0) arrived.
func (f *fakeCamera) eventTime(name string, n int) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range f.talkEvents {
		if e == name {
			if n == 0 {
				return f.eventAt[i]
			}
			n--
		}
	}
	return time.Time{}
}

func (f *fakeCamera) violationList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.violations...)
}
