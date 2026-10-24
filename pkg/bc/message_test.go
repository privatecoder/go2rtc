package bc

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
)

func bcCodec() *Codec {
	c := NewCodec("")
	c.SetCipher(Cipher{Mode: ModeBC})
	return c
}

func TestHeaderModernLogin20(t *testing.T) {
	sample := readTestdata(t, "model_sample_modern_login.bin")

	h, n, err := ParseHeader(sample)
	require.NoError(t, err)
	require.Equal(t, 20, n)
	require.Equal(t, 20, h.Size())
	require.Equal(t, Header{
		MsgID: 1, BodyLen: 145, Class: 0x6614, ResponseCode: 0xdd01, MsgNum: 256,
	}, h)

	// re-encode is byte-identical to the camera's header
	require.Equal(t, sample[:20], h.Append(nil))

	m, used, err := bcCodec().Decode(sample)
	require.NoError(t, err)
	require.Equal(t, len(sample), used)
	require.Nil(t, m.Extension)
	require.Equal(t, "9E6D1FCB9E69846D", m.Body.Encryption.Nonce)
	require.Equal(t, "md5", m.Body.Encryption.Type)
}

func TestHeaderLegacyLogin(t *testing.T) {
	sample := readTestdata(t, "model_sample_legacy_login.bin")

	h, n, err := ParseHeader(sample)
	require.NoError(t, err)
	require.Equal(t, 20, n)
	require.Equal(t, uint32(1836), h.BodyLen)
	require.Equal(t, uint16(0xdc01), h.ResponseCode)
	require.Equal(t, uint16(ClassLegacy), h.Class)

	c := bcCodec()
	m, _, err := c.Decode(sample)
	require.NoError(t, err)
	require.Equal(t, "21232F297A57A5A743894A0E4A801FC\x00", m.Legacy.Username)
	require.Equal(t, string(make([]byte, 32)), m.Legacy.Password)

	out, err := c.Encode(m)
	require.NoError(t, err)
	require.Equal(t, sample, out)

	_, err = c.Encode(&Message{MsgID: 1, Class: ClassLegacy, Legacy: &LegacyLogin{"short", "short"}})
	require.Error(t, err)

	// exactly one of the two legacy variants
	_, err = c.Encode(&Message{MsgID: 1, Class: ClassLegacy})
	require.Error(t, err)
	_, err = c.Encode(&Message{MsgID: 1, Class: ClassLegacy, Legacy: m.Legacy, LegacyUpgrade: true})
	require.Error(t, err)
}

// The login starts with a header-only legacy message whose response code is
// the highest encryption the client offers (neolink login.rs).
func TestLegacyUpgrade(t *testing.T) {
	for code, hexWant := range map[uint16]string{
		0xdc00: "f0debc0a" + "01000000" + "00000000" + "00" + "00" + "0100" + "00dc" + "1465",
		0xdc01: "f0debc0a" + "01000000" + "00000000" + "00" + "00" + "0100" + "01dc" + "1465",
		0xdc12: "f0debc0a" + "01000000" + "00000000" + "00" + "00" + "0100" + "12dc" + "1465",
	} {
		msg := &Message{MsgID: MsgLogin, MsgNum: 1, ResponseCode: code, Class: ClassLegacy, LegacyUpgrade: true}
		b, err := NewCodec("").Encode(msg)
		require.NoError(t, err)
		require.Equal(t, hexWant, hex.EncodeToString(b), "response code %#x", code)

		h, n, err := ParseHeader(b)
		require.NoError(t, err)
		require.Equal(t, 20, n)
		require.Zero(t, h.BodyLen)

		got, used, err := bcCodec().Decode(b)
		require.NoError(t, err)
		require.Equal(t, 20, used)
		require.True(t, got.LegacyUpgrade)
		require.Nil(t, got.Legacy)
		require.Equal(t, code, got.ResponseCode)

		again, err := NewCodec("").Encode(got)
		require.NoError(t, err)
		require.Equal(t, b, again)
	}
}

func TestFullAESEncryptLenBounds(t *testing.T) {
	cc := NewAESCipher(ModeFullAES, "n", "pw")
	media := bytes.Repeat([]byte{7}, 40)

	decode := func(encryptLen uint32) (*Message, error) {
		ext := NewExtension()
		ext.BinaryData, ext.EncryptLen = new(uint32), &encryptLen
		*ext.BinaryData = 1
		b, err := encodeMessage(&Message{
			MsgID: MsgVideo, Class: ClassModern, MsgNum: 4, Extension: ext, Binary: cc.Encrypt(0, media),
		}, cc)
		require.NoError(t, err)
		c := NewCodec("pw")
		c.SetCipher(cc)
		m, _, err := c.Decode(b)
		return m, err
	}

	n := uint32(len(media))
	m, err := decode(n)
	require.NoError(t, err)
	require.Equal(t, media, m.Binary)

	m, err = decode(10)
	require.NoError(t, err)
	require.Equal(t, media[:10], m.Binary)

	for _, bad := range []uint32{n + 1, 0x80000000, 0xffffffff} {
		_, err = decode(bad)
		require.ErrorContains(t, err, "encryptLen", "encryptLen %#x", bad)
	}
}

func TestHeaderLoginSuccess24(t *testing.T) {
	sample := readTestdata(t, "modern_login_success.bin")

	h, n, err := ParseHeader(sample)
	require.NoError(t, err)
	require.Equal(t, 24, n)
	require.Equal(t, Header{MsgID: 1, BodyLen: 2949, ResponseCode: 200, Class: 0}, h)
	require.Equal(t, sample[:24], h.Append(nil))

	m, _, err := bcCodec().Decode(sample)
	require.NoError(t, err)
	require.Nil(t, m.Extension)
	require.NotNil(t, m.Body.DeviceInfo)
	require.Equal(t, 3840, int(m.Body.DeviceInfo.Resolution.Width))
	require.Equal(t, 2160, int(m.Body.DeviceInfo.Resolution.Height))
}

func TestLoginFailed(t *testing.T) {
	m, _, err := bcCodec().Decode(readTestdata(t, "modern_login_failed.bin"))
	require.NoError(t, err)
	require.Equal(t, uint16(400), m.ResponseCode)
	require.Nil(t, m.Extension)
	require.Nil(t, m.Body)
	require.Nil(t, m.Binary)
}

func TestUnknownEncryption(t *testing.T) {
	// 0xdd03 from an Argus 2
	sample := readTestdata(t, "battery_enc.bin")
	h, _, err := ParseHeader(sample)
	require.NoError(t, err)
	require.Equal(t, uint16(0xdd03), h.ResponseCode)
	require.Equal(t, uint32(175), h.BodyLen)

	_, _, err = bcCodec().Decode(sample)
	require.ErrorIs(t, err, UnknownEncryptionError(3))
}

func TestVideoStartBinary(t *testing.T) {
	s1 := readTestdata(t, "modern_video_start1.bin")
	s2 := readTestdata(t, "modern_video_start2.bin")

	c := bcCodec()
	m1, _, err := c.Decode(s1)
	require.NoError(t, err)
	require.Equal(t, uint32(MsgVideo), m1.MsgID)
	require.Equal(t, uint32(1), *m1.Extension.BinaryData)
	require.Len(t, m1.Binary, 32)
	require.Nil(t, m1.Body)

	// the continuation carries no extension, binary mode is remembered by msg_num
	m2, _, err := c.Decode(s2)
	require.NoError(t, err)
	require.Nil(t, m2.Extension)
	require.Equal(t, m1.MsgNum, m2.MsgNum)
	require.Len(t, m2.Binary, 30344)

	// without that state the same bytes are not valid xml
	_, _, err = bcCodec().Decode(s2)
	require.Error(t, err)
}

func TestB800Streams(t *testing.T) {
	tests := []struct {
		file    string
		channel uint8
		handle  uint32
		stream  string
	}{
		{"xml_mainstream_b800.bin", 138, 0, "mainStream"},
		{"xml_substream_b800.bin", 143, 256, "subStream"},
		{"xml_externstream_b800.bin", 0x8c, 1024, "externStream"},
	}
	for _, tc := range tests {
		t.Run(tc.stream, func(t *testing.T) {
			m, _, err := bcCodec().Decode(readTestdata(t, tc.file))
			require.NoError(t, err)
			require.Equal(t, uint32(MsgVideo), m.MsgID)
			require.Equal(t, tc.channel, m.ChannelID)
			require.Equal(t, uint16(ClassModern24), m.Class)
			require.Nil(t, m.Extension)
			p := m.Body.Preview
			require.Equal(t, "1.1", p.Version)
			require.Equal(t, uint8(0), p.ChannelID)
			require.Equal(t, tc.handle, p.Handle)
			require.Equal(t, tc.stream, p.StreamType)
		})
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	ch := uint8(0)
	in := &Message{
		MsgID: MsgVideo, ChannelID: 3, MsgNum: 9, Class: ClassModern24,
		Extension: &Extension{Version: XMLVersion, ChannelID: &ch},
		Body: &Body{Preview: &Preview{
			Version: XMLVersion, Handle: 256, StreamType: "subStream",
		}},
	}

	for _, cc := range []Cipher{
		{Mode: ModeNone},
		{Mode: ModeBC},
		NewAESCipher(ModeAES, "nonce", "pass"),
		NewAESCipher(ModeFullAES, "nonce", "pass"),
	} {
		c := NewCodec("pass")
		c.SetCipher(cc)
		b, err := c.Encode(in)
		require.NoError(t, err)

		h, _, err := ParseHeader(b)
		require.NoError(t, err)
		require.Equal(t, uint32(len(b)-24), h.BodyLen)
		require.Greater(t, h.PayloadOffset, uint32(0))

		out, n, err := c.Decode(b)
		require.NoError(t, err)
		require.Equal(t, len(b), n)
		require.Equal(t, uint8(0), *out.Extension.ChannelID)
		require.Equal(t, "subStream", out.Body.Preview.StreamType)
		require.Equal(t, uint32(256), out.Body.Preview.Handle)
	}
}

func TestEncodeLoginDowngradesAES(t *testing.T) {
	c := NewCodec("pass")
	c.SetCipher(NewAESCipher(ModeAES, "nonce", "pass"))
	msg := &Message{
		MsgID: MsgLogin, Class: ClassModern24,
		Body: &Body{LoginUser: &LoginUser{Version: XMLVersion, UserName: "u", Password: "p", UserVer: 1}},
	}
	b, err := c.Encode(msg)
	require.NoError(t, err)

	// readable with BC, which is what the camera expects before AES starts
	plain := Cipher{Mode: ModeBC}.Decrypt(0, b[24:])
	require.Contains(t, string(plain), "<userName>u</userName>")
}

func TestLoginReplySwitchesCipher(t *testing.T) {
	const nonce = "9E6D1FCB9E69846D"
	reply := &Message{
		MsgID: MsgLogin, ResponseCode: 0xdd02, Class: ClassModern20,
		Body: &Body{Encryption: &Encryption{Version: XMLVersion, Type: "md5", Nonce: nonce}},
	}
	raw, err := encodeMessage(reply, Cipher{Mode: ModeBC})
	require.NoError(t, err)

	c := NewCodec("123456")
	m, _, err := c.Decode(raw)
	require.NoError(t, err)
	require.Equal(t, nonce, m.Body.Encryption.Nonce)

	got := c.Cipher()
	require.Equal(t, ModeAES, got.Mode)
	require.Equal(t, "0D119EB629684BDF", string(got.Key[:]))

	// full aes
	reply.ResponseCode = 0xdd12
	raw, _ = encodeMessage(reply, Cipher{Mode: ModeBC})
	c = NewCodec("123456")
	_, _, err = c.Decode(raw)
	require.NoError(t, err)
	require.Equal(t, ModeFullAES, c.Cipher().Mode)

	// unencrypted
	reply.ResponseCode = 0xdd00
	raw, _ = encodeMessage(reply, Cipher{Mode: ModeNone})
	c = NewCodec("123456")
	_, _, err = c.Decode(raw)
	require.NoError(t, err)
	require.Equal(t, ModeNone, c.Cipher().Mode)
}

func TestFullAESBinary(t *testing.T) {
	c := NewCodec("pw")
	cc := NewAESCipher(ModeFullAES, "n", "pw")
	c.SetCipher(cc)

	media := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 8)
	encLen := uint32(len(media))
	ext := &Extension{Version: XMLVersion, BinaryData: new(uint32), EncryptLen: &encLen}
	*ext.BinaryData = 1

	enc := cc.Encrypt(0, media)
	b, err := encodeMessage(&Message{MsgID: MsgVideo, Class: ClassModern, MsgNum: 4, Extension: ext, Binary: enc}, cc)
	require.NoError(t, err)

	m, _, err := c.Decode(b)
	require.NoError(t, err)
	require.Equal(t, media, m.Binary)
}

func TestDecodeLimits(t *testing.T) {
	hdr := func(bodyLen uint32, class uint16, off uint32) []byte {
		h := Header{MsgID: 1, BodyLen: bodyLen, Class: class, PayloadOffset: off}
		return h.Append(nil)
	}

	_, _, err := ParseHeader(hdr(maxBodyLen+1, ClassModern20, 0))
	require.ErrorIs(t, err, ErrBodyLen)
	_, _, err = ParseHeader(hdr(maxBodyLen, ClassModern20, 0))
	require.NoError(t, err)

	// payload_offset past body_len
	b := append(hdr(4, ClassModern24, 100), 0, 0, 0, 0)
	_, _, err = bcCodec().Decode(b)
	require.ErrorIs(t, err, ErrPayloadOff)

	// incomplete, byte by byte
	sample := readTestdata(t, "modern_login_success.bin")
	for i := 0; i < len(sample); i += 97 {
		_, _, err = bcCodec().Decode(sample[:i])
		require.ErrorIs(t, err, ErrIncomplete, "prefix %d", i)
	}

	_, _, err = bcCodec().Decode([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	require.ErrorIs(t, err, ErrBadMagic)
}

func TestReverseMagic(t *testing.T) {
	sample := bytes.Clone(readTestdata(t, "model_sample_modern_login.bin"))
	copy(sample, []byte{0xa0, 0xcb, 0xed, 0x0f})
	m, _, err := bcCodec().Decode(sample)
	require.NoError(t, err)
	require.NotNil(t, m.Body.Encryption)
}

func ack(num uint16) []byte {
	h := Header{MsgID: MsgPing, MsgNum: num, ResponseCode: 200, Class: ClassModern}
	return h.Append(nil)
}

func TestReaderResyncGarbage(t *testing.T) {
	sample := readTestdata(t, "modern_video_start1.bin")

	// A binary message found after garbage is only handed out once the bytes
	// behind it hold another header.
	buf := append(bytes.Repeat([]byte{0xAA}, 37), sample...)
	buf = append(buf, ack(5)...)
	r := NewReader(bytes.NewReader(buf), bcCodec())
	m, err := r.ReadMessage()
	require.NoError(t, err)
	require.Len(t, m.Binary, 32)
	require.Equal(t, 0, r.Skipped(), "counter resets after a clean recovery")

	m, err = r.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, uint16(5), m.MsgNum)

	_, err = r.ReadMessage()
	require.ErrorIs(t, err, io.EOF)

	// at the end of the data it can never be confirmed
	r = NewReader(bytes.NewReader(append(bytes.Repeat([]byte{0xAA}, 37), sample...)), bcCodec())
	_, err = r.ReadMessage()
	require.ErrorIs(t, err, io.EOF)
}

// A false header inside damaged data must not swallow the genuine message
// behind it, nor change the codec state.
func TestReaderRecoverFalseBinaryHeader(t *testing.T) {
	c := bcCodec()
	c.binary[7] = true // msg_num 7 is a running binary stream

	// a valid looking video header for msg_num 7 with a 24 byte body, which
	// is exactly the genuine ack that follows it
	fake := Header{MsgID: MsgVideo, BodyLen: 24, MsgNum: 7, Class: ClassModern}
	stream := append(bytes.Repeat([]byte{0x55}, 13), fake.Append(nil)...)
	stream = append(stream, ack(3)...)

	for _, rd := range []io.Reader{
		bytes.NewReader(stream),
		iotest.OneByteReader(bytes.NewReader(stream)),
	} {
		r := NewReader(rd, c)
		m, err := r.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, uint32(MsgPing), m.MsgID, "genuine ack lost")
		require.Equal(t, uint16(3), m.MsgNum)
		require.Nil(t, m.Binary)
		_, err = r.ReadMessage()
		require.ErrorIs(t, err, io.EOF)
	}
}

func TestReaderRecoverImplausibleHeader(t *testing.T) {
	for name, h := range map[string]Header{
		"class":     {MsgID: MsgPing, Class: 0x1234},
		"id":        {MsgID: 0xdeadbeef, Class: ClassModern},
		"offset":    {MsgID: MsgPing, BodyLen: 4, PayloadOffset: 100, Class: ClassModern24},
		"body size": {MsgID: MsgPing, BodyLen: maxBodyLen + 1, Class: ClassModern20},
	} {
		t.Run(name, func(t *testing.T) {
			bad := h.Append(nil)
			if h.BodyLen > 0 && h.BodyLen < 100 {
				bad = append(bad, 0, 0, 0, 0)
			}
			stream := append(append([]byte{9, 9, 9, 9, 9}, bad...), ack(4)...)
			r := NewReader(bytes.NewReader(stream), bcCodec())
			m, err := r.ReadMessage()
			require.NoError(t, err)
			require.Equal(t, uint16(4), m.MsgNum, "returned the implausible header")
		})
	}

	// the same bytes without a genuine message behind them are never returned
	bad := Header{MsgID: 0xdeadbeef, Class: 0x1234}
	r := NewReader(bytes.NewReader(append([]byte{9, 9, 9}, bad.Append(nil)...)), bcCodec())
	_, err := r.ReadMessage()
	require.ErrorIs(t, err, io.EOF)
}

func TestReaderRecoverFalseHugeBody(t *testing.T) {
	// a false header declaring the 16 MiB maximum, then a genuine ack
	fake := Header{MsgID: MsgVideo, BodyLen: maxBodyLen, MsgNum: 9, Class: ClassModern}
	stream := append(bytes.Repeat([]byte{0x77}, 21), fake.Append(nil)...)
	stream = append(stream, ack(8)...)

	// finite stream: used to end in EOF with the ack lost
	r := NewReader(bytes.NewReader(stream), bcCodec())
	m, err := r.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, uint16(8), m.MsgNum)

	// live stream: no EOF, the reader must not wait for 16 MiB
	pr, pw := io.Pipe()
	r = NewReader(pr, bcCodec())
	go func() {
		pw.Write(stream)
	}()
	done := make(chan *Message, 1)
	go func() {
		m, _ := r.ReadMessage()
		done <- m
	}()
	select {
	case m := <-done:
		require.NotNil(t, m)
		require.Equal(t, uint16(8), m.MsgNum)
	case <-time.After(3 * time.Second):
		t.Fatal("reader is waiting for the body of a false header")
	}
	pw.Close()
}

func TestReaderBudgetNotResetByJump(t *testing.T) {
	// a genuine message far behind 100 KB of garbage: the skip itself is over
	// budget, a later clean decode must not hide that
	stream := append(bytes.Repeat([]byte{0x11}, 100*1024), ack(1)...)
	r := NewReader(bytes.NewReader(stream), bcCodec())
	_, err := r.ReadMessage()
	require.ErrorIs(t, err, ErrResync)
}

func TestReaderResyncBetweenMessages(t *testing.T) {
	a := readTestdata(t, "model_sample_modern_login.bin")
	b := readTestdata(t, "xml_mainstream_b800.bin")

	// garbage that looks like a lost udp packet, including a fake header
	// whose body fails to parse and a partial magic at the end
	bad := Header{MsgID: 3, BodyLen: 10, Class: ClassModern20}
	junk := append(bad.Append(nil), []byte("not xml!!!")...)
	junk = append(junk, 0xf0, 0xde, 0xbc)

	var stream []byte
	stream = append(stream, a...)
	stream = append(stream, junk...)
	stream = append(stream, b...)
	stream = append(stream, a...)

	// every read size must give the same result, including one byte at a time
	for _, rd := range []io.Reader{
		bytes.NewReader(stream),
		iotest.OneByteReader(bytes.NewReader(stream)),
		iotest.HalfReader(bytes.NewReader(stream)),
	} {
		r := NewReader(rd, bcCodec())
		var ids []uint32
		for {
			m, err := r.ReadMessage()
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			ids = append(ids, m.MsgID)
		}
		require.Equal(t, []uint32{1, 3, 1}, ids)
	}
}

func TestReaderResyncBudget(t *testing.T) {
	// valid magic followed by a body that never parses, over and over
	bad := Header{MsgID: 3, BodyLen: 8, Class: ClassModern20}
	unit := append(bad.Append(nil), []byte("garbage!")...)
	stream := bytes.Repeat(unit, 6000) // 168 KB of failures

	r := NewReader(bytes.NewReader(stream), bcCodec())
	_, err := r.ReadMessage()
	require.ErrorIs(t, err, ErrResync)
}

func TestExtensionXML(t *testing.T) {
	ext := NewExtension()
	b, err := ext.Marshal()
	require.NoError(t, err)
	require.Equal(t, `<?xml version="1.0" encoding="UTF-8" ?>`+"\n"+`<Extension version="1.1"></Extension>`, string(b))

	one := uint32(1)
	ch := uint8(2)
	ext = &Extension{Version: XMLVersion, BinaryData: &one, ChannelID: &ch}
	b, err = ext.Marshal()
	require.NoError(t, err)
	got, err := ParseExtension(b)
	require.NoError(t, err)
	require.Equal(t, uint32(1), *got.BinaryData)
	require.Equal(t, uint8(2), *got.ChannelID)
	require.Nil(t, got.EncryptLen)
}

func TestTalkXML(t *testing.T) {
	const ability = `<?xml version="1.0" encoding="UTF-8" ?>
<body>
<TalkAbility version="1.1">
<duplexList><duplex>FDX</duplex></duplexList>
<audioStreamModeList><audioStreamMode>followVideoStream</audioStreamMode></audioStreamModeList>
<audioConfigList><audioConfig>
<priority>0</priority><audioType>adpcm</audioType><sampleRate>16000</sampleRate>
<samplePrecision>16</samplePrecision><lengthPerEncoder>1024</lengthPerEncoder><soundTrack>mono</soundTrack>
</audioConfig></audioConfigList>
<unknownElement>x</unknownElement>
</TalkAbility>
</body>`
	b, err := ParseBody([]byte(ability))
	require.NoError(t, err)
	ta := b.TalkAbility
	require.Equal(t, "FDX", ta.DuplexList[0].Duplex)
	require.Equal(t, "followVideoStream", ta.AudioStreamModeList[0].AudioStreamMode[0])
	cfg := ta.AudioConfigList[0].AudioConfig
	require.Equal(t, "adpcm", cfg.AudioType)
	require.Equal(t, uint16(16000), cfg.SampleRate)
	require.Equal(t, uint16(1024), cfg.LengthPerEncoder)

	cfg.Priority = nil
	out, err := (&Body{TalkConfig: &TalkConfig{
		Version: XMLVersion, Duplex: "FDX", AudioStreamMode: "followVideoStream", AudioConfig: cfg,
	}}).Marshal()
	require.NoError(t, err)
	require.Contains(t, string(out), "<lengthPerEncoder>1024</lengthPerEncoder>")
	require.NotContains(t, string(out), "priority")
	back, err := ParseBody(out)
	require.NoError(t, err)
	require.Equal(t, cfg, back.TalkConfig.AudioConfig)
}

func TestLoginXML(t *testing.T) {
	b, err := (&Body{
		LoginUser: &LoginUser{Version: XMLVersion, UserName: ModernHash("admin" + "N"), Password: ModernHash("pw" + "N"), UserVer: 1},
		LoginNet:  &LoginNet{Version: XMLVersion, Type: "LAN"},
	}).Marshal()
	require.NoError(t, err)
	got, err := ParseBody(b)
	require.NoError(t, err)
	require.Equal(t, uint32(1), got.LoginUser.UserVer)
	require.Len(t, got.LoginUser.Password, 31)
	require.Equal(t, "LAN", got.LoginNet.Type)
	require.Equal(t, uint16(0), got.LoginNet.UDPPort)
}
