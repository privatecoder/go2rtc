package bc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	MagicHeader    = 0x0abcdef0
	MagicHeaderRev = 0x0fedcba0 // seen on some JPEG replies, accepted on decode only

	// maxBodyLen caps body_len before any buffering, a bogus header must not
	// make us wait for gigabytes.
	maxBodyLen = 16 * 1024 * 1024
	// maxResync is how many bytes may be skipped without a clean decode
	// before the stream is considered broken.
	maxResync = 64 * 1024
)

const (
	MsgLogin        = 1
	MsgLogout       = 2
	MsgVideo        = 3
	MsgVideoStop    = 4
	MsgTalkAbility  = 10
	MsgTalkReset    = 11
	MsgGetEnc       = 56
	MsgVersion      = 80
	MsgPing         = 93
	MsgUID          = 114
	MsgStreamInfo   = 146
	MsgTalkConfig   = 201
	MsgTalk         = 202
	MsgUDPKeepAlive = 234
)

// Header classes. Only 0x6414 and 0x0000 have the extra payload_offset word.
const (
	ClassLegacy   = 0x6514 // 20 bytes, legacy login
	ClassModern20 = 0x6614 // 20 bytes, reply to an encrypted login
	ClassModern24 = 0x6414 // 24 bytes, re-sent login and most requests
	ClassModern   = 0x0000 // 24 bytes, most replies
)

var (
	// ErrIncomplete means more bytes are needed to decode a whole message.
	ErrIncomplete = errors.New("bc: incomplete message")
	ErrBadMagic   = errors.New("bc: invalid magic")
	ErrBodyLen    = errors.New("bc: body_len too large")
	ErrPayloadOff = errors.New("bc: payload_offset exceeds body_len")
	ErrResync     = errors.New("bc: resync gave up")
)

// Header is the fixed part of a message, little-endian on the wire.
type Header struct {
	MsgID         uint32
	BodyLen       uint32
	ChannelID     uint8 // also the per-message XOR offset
	StreamType    uint8 // 0 = clear, 1 = fluent
	MsgNum        uint16
	ResponseCode  uint16
	Class         uint16
	PayloadOffset uint32 // only if HasPayloadOffset
}

func HasPayloadOffset(class uint16) bool {
	return class == ClassModern24 || class == ClassModern
}

func (h *Header) Size() int {
	if HasPayloadOffset(h.Class) {
		return 24
	}
	return 20
}

// Append encodes the header, always with the canonical magic.
func (h *Header) Append(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, MagicHeader)
	b = binary.LittleEndian.AppendUint32(b, h.MsgID)
	b = binary.LittleEndian.AppendUint32(b, h.BodyLen)
	b = append(b, h.ChannelID, h.StreamType)
	b = binary.LittleEndian.AppendUint16(b, h.MsgNum)
	b = binary.LittleEndian.AppendUint16(b, h.ResponseCode)
	b = binary.LittleEndian.AppendUint16(b, h.Class)
	if HasPayloadOffset(h.Class) {
		b = binary.LittleEndian.AppendUint32(b, h.PayloadOffset)
	}
	return b
}

// ParseHeader decodes a header from the start of b and returns its size.
// ErrIncomplete is returned only if the bytes seen so far are plausible.
func ParseHeader(b []byte) (h Header, n int, err error) {
	if len(b) < 4 {
		return h, 0, ErrIncomplete
	}
	if m := binary.LittleEndian.Uint32(b); m != MagicHeader && m != MagicHeaderRev {
		return h, 0, ErrBadMagic
	}
	if len(b) < 12 {
		return h, 0, ErrIncomplete
	}
	h.MsgID = binary.LittleEndian.Uint32(b[4:])
	h.BodyLen = binary.LittleEndian.Uint32(b[8:])
	if h.BodyLen > maxBodyLen {
		return h, 0, ErrBodyLen
	}
	if len(b) < 20 {
		return h, 0, ErrIncomplete
	}
	h.ChannelID = b[12]
	h.StreamType = b[13]
	h.MsgNum = binary.LittleEndian.Uint16(b[14:])
	h.ResponseCode = binary.LittleEndian.Uint16(b[16:])
	h.Class = binary.LittleEndian.Uint16(b[18:])
	n = 20
	if HasPayloadOffset(h.Class) {
		if len(b) < 24 {
			return h, 0, ErrIncomplete
		}
		h.PayloadOffset = binary.LittleEndian.Uint32(b[20:])
		n = 24
	}
	return h, n, nil
}

// LegacyLogin is the body of the class 0x6514 login: two 32-byte fields,
// padded to a 1836-byte body.
type LegacyLogin struct {
	Username, Password string
}

const legacyLoginBodyLen = 1836

// Message is one decoded or to-be-encoded packet. For modern classes it has
// an optional Extension and at most one of Body (XML) and Binary.
type Message struct {
	MsgID        uint32
	ChannelID    uint8
	StreamType   uint8
	MsgNum       uint16
	ResponseCode uint16
	Class        uint16

	Extension *Extension
	Body      *Body
	Binary    []byte
	Legacy    *LegacyLogin // class 0x6514, msg 1 with credentials
	// LegacyUpgrade is the header-only class 0x6514 login request that starts
	// every session, ResponseCode 0xdc00, 0xdc01 or 0xdc12 selects the
	// maximum encryption. Exactly one of Legacy and LegacyUpgrade is set on
	// a legacy message.
	LegacyUpgrade bool
}

// Codec holds the per-connection state of the control channel: negotiated
// cipher and which msg_nums are in binary mode (continuation packets of a
// binary message carry no extension). It is safe for concurrent use, the
// reader decodes while writers encode.
type Codec struct {
	password string // derives the AES key from the login reply nonce, immutable

	mu     sync.RWMutex
	cipher Cipher
	binary map[uint16]bool
}

func NewCodec(password string) *Codec {
	return &Codec{password: password, binary: map[uint16]bool{}}
}

// SetCipher overrides the cipher, for tests and for transports that know it.
func (c *Codec) SetCipher(cc Cipher) {
	c.mu.Lock()
	c.cipher = cc
	c.mu.Unlock()
}

func (c *Codec) Cipher() Cipher {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cipher
}

func isAES(m Mode) bool { return m == ModeAES || m == ModeFullAES }

// Encode serializes m. Msg 1 (login) is downgraded to BC encryption while the
// session is AES, because the nonce is not established yet.
func (c *Codec) Encode(m *Message) ([]byte, error) {
	cc := c.Cipher()
	if m.MsgID == MsgLogin && isAES(cc.Mode) {
		cc = Cipher{Mode: ModeBC}
	}
	return encodeMessage(m, cc)
}

func encodeMessage(m *Message, cc Cipher) ([]byte, error) {
	h := Header{
		MsgID: m.MsgID, ChannelID: m.ChannelID, StreamType: m.StreamType,
		MsgNum: m.MsgNum, ResponseCode: m.ResponseCode, Class: m.Class,
	}

	var body []byte
	if m.Class == ClassLegacy {
		switch {
		case m.LegacyUpgrade && m.Legacy == nil:
			// header only
		case m.Legacy != nil && !m.LegacyUpgrade:
			if len(m.Legacy.Username) != 32 || len(m.Legacy.Password) != 32 {
				return nil, errors.New("bc: legacy login fields must be 32 bytes")
			}
			body = make([]byte, legacyLoginBodyLen)
			copy(body, m.Legacy.Username)
			copy(body[32:], m.Legacy.Password)
		default:
			return nil, errors.New("bc: legacy message needs either Legacy or LegacyUpgrade")
		}
	} else {
		if m.Extension != nil {
			b, err := m.Extension.Marshal()
			if err != nil {
				return nil, err
			}
			body = cc.Encrypt(uint32(m.ChannelID), b)
			if HasPayloadOffset(m.Class) {
				h.PayloadOffset = uint32(len(body))
			}
		}
		switch {
		case m.Body != nil:
			b, err := m.Body.Marshal()
			if err != nil {
				return nil, err
			}
			body = append(body, cc.Encrypt(uint32(m.ChannelID), b)...)
		case m.Binary != nil:
			body = append(body, m.Binary...) // binary is never encrypted
		}
	}

	h.BodyLen = uint32(len(body))
	out := make([]byte, 0, 24+len(body))
	out = h.Append(out)
	return append(out, body...), nil
}

// Decode decodes one message from the start of buf and returns the number of
// bytes consumed. ErrIncomplete means wait for more data; any other error
// means buf does not start with a valid message (see Reader for resync).
func (c *Codec) Decode(buf []byte) (*Message, int, error) {
	m, n, err := c.parse(buf)
	if err != nil {
		return nil, 0, err
	}
	if err = c.update(m); err != nil {
		return nil, 0, err
	}
	return m, n, nil
}

// parse decodes a message without applying its side effects to the codec.
func (c *Codec) parse(buf []byte) (*Message, int, error) {
	h, hn, err := ParseHeader(buf)
	if err != nil {
		return nil, 0, err
	}
	total := hn + int(h.BodyLen)
	if len(buf) < total {
		return nil, 0, ErrIncomplete
	}
	body := buf[hn:total]

	m := &Message{
		MsgID: h.MsgID, ChannelID: h.ChannelID, StreamType: h.StreamType,
		MsgNum: h.MsgNum, ResponseCode: h.ResponseCode, Class: h.Class,
	}

	if h.Class == ClassLegacy {
		if h.MsgID == MsgLogin {
			switch {
			case len(body) == 0:
				m.LegacyUpgrade = true
			case len(body) >= 64:
				m.Legacy = &LegacyLogin{Username: string(body[:32]), Password: string(body[32:64])}
			}
		}
		return m, total, nil
	}

	c.mu.RLock()
	ctx := c.cipher
	inBin := c.binary[h.MsgNum]
	c.mu.RUnlock()

	extLen := h.PayloadOffset // zero if the class has no offset word
	if extLen > h.BodyLen {
		return nil, 0, ErrPayloadOff
	}
	extBuf, payBuf := body[:extLen], body[extLen:]

	var binaryExt bool
	var encryptLen *uint32
	if extLen > 0 {
		raw := extBuf
		if ctx.Mode != ModeNone {
			raw = ctx.Decrypt(uint32(h.ChannelID), extBuf)
		}
		m.Extension, err = ParseExtension(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("bc: extension xml: %w", err)
		}
		if m.Extension.BinaryData != nil && *m.Extension.BinaryData == 1 {
			binaryExt = true
			encryptLen = m.Extension.EncryptLen
		}
	}

	if len(payBuf) > 0 {
		pc := payloadCipher(h, ctx)
		dec := pc.Decrypt(uint32(h.ChannelID), payBuf)
		if inBin || binaryExt {
			if ctx.Mode == ModeFullAES && encryptLen != nil {
				// compared as uint64: on 32-bit targets int(0xffffffff) is -1
				if uint64(*encryptLen) > uint64(len(dec)) {
					return nil, 0, errors.New("bc: encryptLen exceeds payload")
				}
				m.Binary = dec[:*encryptLen]
			} else {
				m.Binary = append([]byte(nil), payBuf...)
			}
		} else {
			m.Body, err = ParseBody(dec)
			if err != nil {
				return nil, 0, fmt.Errorf("bc: payload xml: %w", err)
			}
		}
	}

	return m, total, nil
}

// payloadCipher picks the cipher for the XML payload. During login the
// maximum is BC encryption, the nonce has not been exchanged yet.
func payloadCipher(h Header, ctx Cipher) Cipher {
	if h.MsgID != MsgLogin {
		return ctx
	}
	if h.ResponseCode>>8 == 0xdd {
		if h.ResponseCode&0xff == 0 {
			return Cipher{Mode: ModeNone}
		}
		return Cipher{Mode: ModeBC}
	}
	if isAES(ctx.Mode) {
		return Cipher{Mode: ModeBC}
	}
	return ctx
}

// update applies the side effects of a decoded message: the encryption
// negotiated by a login reply and binary mode tracking.
func (c *Codec) update(m *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if m.MsgID == MsgLogin && m.ResponseCode>>8 == 0xdd && m.Body != nil && m.Body.Encryption != nil {
		nonce := m.Body.Encryption.Nonce
		switch m.ResponseCode & 0xff {
		case 0x00:
			c.cipher = Cipher{Mode: ModeNone}
		case 0x01:
			c.cipher = Cipher{Mode: ModeBC}
		case 0x02:
			c.cipher = NewAESCipher(ModeAES, nonce, c.password)
		case 0x12:
			c.cipher = NewAESCipher(ModeFullAES, nonce, c.password)
		default:
			return UnknownEncryptionError(m.ResponseCode & 0xff)
		}
	}
	if e := m.Extension; e != nil && e.BinaryData != nil {
		if *e.BinaryData == 0 {
			delete(c.binary, m.MsgNum)
		} else {
			c.binary[m.MsgNum] = true
		}
	}
	return nil
}

func (c *Codec) isBinary(num uint16) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.binary[num]
}

// clearBinary forgets the binary mode of a msg_num whose stream has ended.
func (c *Codec) clearBinary(num uint16) {
	c.mu.Lock()
	delete(c.binary, num)
	c.mu.Unlock()
}

// UnknownEncryptionError is returned by Reader for a login reply that
// selects an encryption mode we do not know.
type UnknownEncryptionError byte

func (e UnknownEncryptionError) Error() string {
	return fmt.Sprintf("bc: unknown encryption mode 0x%02x", byte(e))
}

// Recovery limits, see Reader.
const (
	recoverBigBody = 256 * 1024 // a candidate claiming more is dropped once another header shows up inside it
	maxKnownMsgID  = 512        // neolink's ids stay below this
)

// plausible is the sanity check applied to a header found while recovering:
// a known class, an id from the known range and a payload offset that fits.
func plausible(h Header) bool {
	switch h.Class {
	case ClassLegacy, ClassModern20, ClassModern24, ClassModern:
	default:
		return false
	}
	return h.MsgID < maxKnownMsgID && h.PayloadOffset <= h.BodyLen
}

// Reader splits a byte stream into messages. TCP framing is reliable, so
// intact messages are decoded purely by their length. After a decode error
// (a lost UDP packet punches a hole in the framing) the reader is in
// recovery: it scans for the next header magic, but a candidate is only
// accepted if
//   - its header is plausible (class, id range, payload offset),
//   - it decodes (an XML body must parse),
//   - for a binary payload, the bytes right behind it hold another plausible
//     header, otherwise the candidate was most likely inside a damaged
//     payload and is dropped without touching the codec state,
//   - and it does not make us wait for a huge body that another header
//     inside it contradicts.
//
// More than 64 KiB skipped without a clean recovery is fatal (ErrResync).
type Reader struct {
	r          io.Reader
	c          *Codec
	buf        []byte
	skipped    int
	recovering bool
	rerr       error
	tmp        [32 * 1024]byte
}

func NewReader(r io.Reader, c *Codec) *Reader {
	return &Reader{r: r, c: c}
}

// Skipped is the number of bytes dropped by the current recovery, 0 when in sync.
func (r *Reader) Skipped() int { return r.skipped }

func (r *Reader) ReadMessage() (*Message, error) {
	for {
		if len(r.buf) > 0 {
			m, wait, err := r.step()
			if err != nil {
				return nil, err
			}
			if m != nil {
				return m, nil
			}
			if !wait {
				continue // made progress
			}
		}

		if r.rerr != nil {
			// the stream ended: an unconfirmed candidate can never be confirmed,
			// drop it and look for genuine messages behind it
			if r.recovering && len(r.buf) > 3 {
				if err := r.skip(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.rerr
		}

		n, err := r.r.Read(r.tmp[:])
		r.buf = append(r.buf, r.tmp[:n]...)
		if err != nil {
			r.rerr = err // handled above once the buffered bytes are used up
		}
	}
}

// step tries to take one message from the buffer. It returns the message, or
// wait if more bytes are needed, or neither if it dropped bytes and the
// caller should try again.
func (r *Reader) step() (m *Message, wait bool, err error) {
	if !r.recovering {
		m, n, err := r.c.parse(r.buf)
		switch {
		case err == nil:
			return r.commit(m, n)
		case err == ErrIncomplete:
			return nil, true, nil
		}
		r.recovering = true
		return nil, false, r.skip()
	}

	h, _, err := ParseHeader(r.buf)
	switch {
	case err == ErrIncomplete:
		return nil, true, nil
	case err != nil || !plausible(h):
		return nil, false, r.skip()
	}

	m, n, err := r.c.parse(r.buf)
	switch {
	case err == ErrIncomplete:
		if h.BodyLen > recoverBigBody && hasMagic(r.buf[1:]) {
			return nil, false, r.skip()
		}
		return nil, true, nil
	case err != nil:
		return nil, false, r.skip()
	}

	if m.Binary != nil {
		next, _, err := ParseHeader(r.buf[n:])
		switch {
		case err == ErrIncomplete:
			return nil, true, nil
		case err != nil || !plausible(next):
			return nil, false, r.skip()
		}
	}
	return r.commit(m, n)
}

func (r *Reader) commit(m *Message, n int) (*Message, bool, error) {
	if err := r.c.update(m); err != nil {
		return nil, false, err
	}
	r.buf = r.buf[n:]
	r.recovering = false
	r.skipped = 0
	return m, false, nil
}

// skip drops the first byte and everything up to the next header magic, it
// always makes progress even when a valid magic sits at offset 0.
func (r *Reader) skip() error {
	drop := len(r.buf) - min(len(r.buf), 3) // no magic: keep a tail, one may straddle reads
	for i := 1; i+4 <= len(r.buf); i++ {
		if isMagic(r.buf[i:]) {
			drop = i
			break
		}
	}
	r.skipped += drop
	r.buf = r.buf[drop:]
	if r.skipped > maxResync {
		return fmt.Errorf("%w after %d bytes", ErrResync, r.skipped)
	}
	return nil
}

func isMagic(b []byte) bool {
	m := binary.LittleEndian.Uint32(b)
	return m == MagicHeader || m == MagicHeaderRev
}

func hasMagic(b []byte) bool {
	for i := 0; i+4 <= len(b); i++ {
		if isMagic(b[i:]) {
			return true
		}
	}
	return false
}
