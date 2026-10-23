package udp

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func sample(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestXMLCrypto(t *testing.T) {
	// neolink's sample, tid 87
	enc := sample(t, "xml_crypto_sample1.bin")
	plain := sample(t, "xml_crypto_sample1_plaintext.bin")
	require.Equal(t, plain, xmlCrypt(87, enc))
	require.Equal(t, enc, xmlCrypt(87, plain))

	// the tid is added to the key words with wrapping
	zeros := make([]byte, 64)
	require.NotPanics(t, func() { xmlCrypt(0xffffffff, zeros) })
	require.Equal(t, zeros, xmlCrypt(5, xmlCrypt(5, zeros)))
	require.NotEqual(t, xmlCrypt(5, zeros), xmlCrypt(6, zeros))
}

func TestChecksum(t *testing.T) {
	// the CRC of every captured discovery packet is over its encrypted payload
	for _, name := range []string{"udp_negotiate_disc.bin", "udp_negotiate_camt.bin", "udp_negotiate_clientt.bin", "udp_negotiate_camcfm.bin"} {
		b := sample(t, name)
		size := binary.LittleEndian.Uint32(b[4:])
		require.Equal(t, binary.LittleEndian.Uint32(b[16:]), checksum(b[20:20+size]), name)
	}
	require.Equal(t, uint32(0), checksum(nil), "initial value 0, no final xor")
}

func TestDiscoverySamples(t *testing.T) {
	tests := []struct {
		file string
		tid  uint32
		want Message
	}{
		{"udp_negotiate_disc.bin", 96, &C2dDisc{CID: 82000, DID: 80}},
		{"udp_negotiate_camt.bin", 113, &D2cT{SID: 62098713, Conn: "local", CID: 82001, DID: 96}},
		{"udp_negotiate_clientt.bin", 1101, &C2dT{SID: 62098713, Conn: "local", CID: 82001, MTU: 1350}},
		{"udp_negotiate_camcfm.bin", 1101, &D2cCfm{SID: 62098713, Conn: "local", CID: 82001, DID: 96, TimeR: new(uint32)}},
		{"udp_multi_0.bin", 87, &D2cT{SID: 62097899, Conn: "local", CID: 82000, DID: 80}},
		{"udp_multi_1.bin", 92, &C2dT{SID: 62097899, Conn: "local", CID: 82000, MTU: 1350}},
		{"udp_multi_2.bin", 92, &D2cCfm{SID: 62097899, Conn: "local", CID: 82000, DID: 80, TimeR: new(uint32)}},
	}
	for _, tc := range tests {
		t.Run(tc.file, func(t *testing.T) {
			b := sample(t, tc.file)
			p, err := Decode(b)
			require.NoError(t, err)
			d, ok := p.(*Discovery)
			require.True(t, ok)
			require.Equal(t, tc.tid, d.TID)
			require.Equal(t, tc.want, d.XML)

			// the camera's own XML layout is reproduced byte for byte
			again, err := d.Marshal()
			require.NoError(t, err)
			require.Equal(t, b, again)
		})
	}

	// every captured discovery packet decodes and re-encodes exactly
	for _, f := range []string{"udp_multi_5.bin", "udp_multi_6.bin", "udp_multi_8.bin", "udp_multi_9.bin"} {
		b := sample(t, f)
		p, err := Decode(b)
		require.NoError(t, err, f)
		again, err := p.Marshal()
		require.NoError(t, err)
		require.Equal(t, b, again, f)
	}
}

func TestAckSample(t *testing.T) {
	b := sample(t, "udp_ack.bin")
	p, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, &Ack{ConnID: 80, GroupID: 0, PacketID: 2439}, p)
	again, _ := p.Marshal()
	require.Equal(t, b, again)
}

func TestDataSamples(t *testing.T) {
	for name, want := range map[string]struct {
		conn int32
		id   uint32
		n    int
	}{
		"udp_data.bin":    {82000, 2439, 1176},
		"udp_multi_3.bin": {80, 0, 1330},
		"udp_multi_4.bin": {80, 1, 526},
		"udp_multi_7.bin": {80, 0, 1330},
	} {
		b := sample(t, name)
		p, err := Decode(b)
		require.NoError(t, err, name)
		d := p.(*Data)
		require.Equal(t, want.conn, d.ConnID, name)
		require.Equal(t, want.id, d.PacketID, name)
		require.Len(t, d.Payload, want.n, name)
		again, err := d.Marshal()
		require.NoError(t, err)
		require.Equal(t, b, again, name)
	}
}

func TestAckWithSelectiveMap(t *testing.T) {
	a := &Ack{ConnID: -5, PacketID: 41, RecvBytesPerSec: 525_000, Payload: []byte{0, 1, 1, 0, 1}}
	b, err := a.Marshal()
	require.NoError(t, err)
	require.Len(t, b, 28+5)
	require.Equal(t, uint32(MagicAck), binary.LittleEndian.Uint32(b))
	require.Equal(t, uint32(525_000), binary.LittleEndian.Uint32(b[20:]), "recv_bytes_per_sec is at 0x14")
	p, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, a, p)

	e, _ := EmptyAck(7).Marshal()
	p, err = Decode(e)
	require.NoError(t, err)
	require.Equal(t, &Ack{ConnID: 7, GroupID: NoAck, PacketID: NoAck}, p)
}

func TestDecodeRejects(t *testing.T) {
	good := sample(t, "udp_negotiate_disc.bin")
	data := sample(t, "udp_data.bin")

	flip := func(b []byte, i int) []byte {
		c := append([]byte(nil), b...)
		c[i] ^= 0xff
		return c
	}

	for name, b := range map[string][]byte{
		"empty":          nil,
		"short magic":    {1, 2},
		"unknown magic":  {1, 2, 3, 4, 5, 6, 7, 8},
		"short disc":     good[:19],
		"truncated disc": good[:len(good)-1],
		"bad checksum":   flip(good, len(good)-1),
		"bad disc const": flip(good, 8),
		"short data":     data[:19],
		"truncated data": data[:len(data)-1],
		"data unknown":   flip(data, 9),
		"short ack":      sample(t, "udp_ack.bin")[:27],
		"ack group":      flip(sample(t, "udp_ack.bin"), 12),
	} {
		_, err := Decode(b)
		require.Error(t, err, name)
	}

	// a valid frame around an XML that is not P2P
	d := &Discovery{TID: 5, XML: &C2dHb{}}
	b, _ := d.Marshal()
	require.NoError(t, func() error { _, err := Decode(b); return err }())
}

func TestXMLMessages(t *testing.T) {
	rev := int32(3)
	tm := uint32(55607)
	sid := uint32(495151439)
	msgs := []Message{
		&C2mQ{UID: "95270000YGAKNWKJ", OS: "MAC"},
		&C2rC{UID: "95270000YGAKNWKJ", Cli: IpPort{"10.202.133.237", 12254}, Relay: IpPort{"18.162.200.47", 58100}, CID: 254000, Family: 4, OS: "MAC", R: &rev},
		&C2rC{UID: "x", CID: -1, Family: 4, OS: "MAC"},
		&C2rCfm{SID: 495151439, Conn: "map", CID: 254000, DID: 735},
		&C2dC{UID: "95270000YGAKNWKJ", Cli: Port{53600}, CID: -376737975, MTU: 1350, OS: "MAC"},
		&C2dT{SID: 1, Conn: "relay", CID: 38000, MTU: 1350},
		&C2dA{SID: 495151439, Conn: "map", CID: 254000, DID: 735, MTU: 1350},
		&D2cCfm{SID: 526020041, Conn: "relay", CID: 38000, DID: 32, TimeR: &tm},
		&D2cCfm{SID: 1, Conn: "local", CID: 2, DID: 3},
		&C2dHb{CID: 1, DID: -2},
		&D2cHb{CID: 1, DID: 2},
		&C2dDisc{CID: 1, DID: 2},
		&D2cDisc{CID: 1, DID: 2},
		&R2cDisc{SID: 9},
		&D2cCr{Timer: Timer{3000, 20000, 60000}, Rsp: 0, CID: -376737975, DID: 49},
		&R2cCr{
			Dev: &IpPort{"192.168.1.100", 57933}, Dmap: &IpPort{"184.22.90.67", 57933},
			Relay: &IpPort{"18.162.200.47", 51134}, Relayt: &IpPort{"18.162.200.47", 9997},
			NAT: "NULL", SID: &sid, Rsp: 0, AC: 495151439,
		},
		&M2cQr{Reg: &IpPort{"18.162.200.47", 58200}, Relay: &IpPort{"18.162.200.47", 58100}},
	}
	for _, m := range msgs {
		b, err := (&Discovery{TID: 77, XML: m}).Marshal()
		require.NoError(t, err)
		p, err := Decode(b)
		require.NoError(t, err, m.XMLName())
		require.Equal(t, m, p.(*Discovery).XML, m.XMLName())
	}
}

func TestParseServerXML(t *testing.T) {
	// layouts seen in neolink's captures, not what we write
	const m2c = `<P2P>
        <M2C_Q_R>
            <reg><ip>18.162.200.47</ip><port>58200</port></reg>
            <relay><ip>18.162.200.47</ip><port>58100</port></relay>
            <log><ip>18.162.200.47</ip><port>57850</port></log>
            <t><ip>18.162.200.47</ip><port>9996</port></t>
            <timer/><retry/><mtu>1350</mtu><debug>251658240</debug><ac>-1700607721</ac><rsp>0</rsp>
        </M2C_Q_R>
    </P2P>`
	m, err := parseXML([]byte(m2c))
	require.NoError(t, err)
	q := m.(*M2cQr)
	require.True(t, q.Reg.Valid())
	require.True(t, q.Relay.Valid())
	a, _ := q.Reg.Addr()
	require.Equal(t, "18.162.200.47:58200", a.String())

	const r2c = `<P2P><R2C_C_R><dmap><ip>184.22.90.67</ip><port>57933</port></dmap><dev><ip>192.168.1.100</ip><port>57933</port></dev>` +
		`<relay><ip>18.162.200.47</ip><port>51134</port></relay><relayt><ip>18.162.200.47</ip><port>9997</port></relayt>` +
		`<nat>NULL</nat><sid>495151439</sid><rsp>0</rsp><ac>495151439</ac></R2C_C_R></P2P>`
	m, err = parseXML([]byte(r2c))
	require.NoError(t, err)
	r := m.(*R2cCr)
	require.Equal(t, uint32(495151439), *r.SID)
	require.True(t, r.Dmap.Valid())

	// failure: no sid
	m, err = parseXML([]byte(`<P2P><R2C_C_R><relay><ip>1.2.3.4</ip><port>5</port></relay><nat>NULL</nat><rsp>-3</rsp><ac>1</ac></R2C_C_R></P2P>`))
	require.NoError(t, err)
	require.Nil(t, m.(*R2cCr).SID)
	require.Equal(t, int32(-3), m.(*R2cCr).Rsp)

	// messages we do not use are an identifiable error, garbage another
	_, err = parseXML([]byte(`<P2P><R2C_T><sid>1</sid></R2C_T></P2P>`))
	require.ErrorIs(t, err, ErrUnknownMessage)
	_, err = parseXML([]byte(`<foo/>`))
	require.Error(t, err)
	_, err = parseXML([]byte(`not xml`))
	require.Error(t, err)

	for _, ip := range []*IpPort{nil, {"", 1}, {"1.2.3.0", 1}, {"1.2.3.4", 0}, {"::1", 5}, {"host", 5}} {
		require.False(t, ip.Valid())
	}
}

func TestXMLEscape(t *testing.T) {
	b, err := (&Discovery{TID: 1, XML: &C2mQ{UID: `a<b>&"`, OS: "MAC"}}).Marshal()
	require.NoError(t, err)
	p, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, `a<b>&"`, p.(*Discovery).XML.(*C2mQ).UID)
}
