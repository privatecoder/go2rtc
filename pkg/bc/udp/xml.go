package udp

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"strconv"
)

// Message is one P2P XML message (<P2P><NAME>...</NAME></P2P>).
type Message interface {
	// XMLName is the element name, C2D_T and so on.
	XMLName() string
}

type IpPort struct {
	IP   string `xml:"ip"`
	Port uint16 `xml:"port"`
}

// Addr is the address as a netip.AddrPort, false if it is not a plain IPv4
// address with a port.
func (p *IpPort) Addr() (netip.AddrPort, bool) {
	if p == nil {
		return netip.AddrPort{}, false
	}
	ip, err := netip.ParseAddr(p.IP)
	if err != nil || !ip.Is4() || p.Port == 0 {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(ip, p.Port), true
}

// Valid is how neolink judges addresses from the servers: IPv4, a port, and
// not a network address (last octet 0).
func (p *IpPort) Valid() bool {
	a, ok := p.Addr()
	return ok && a.Addr().As4()[3] != 0
}

type Port struct {
	Port uint32 `xml:"port"`
}

// Client to middle-man query for a UID (to p2p*.reolink.com:9999).
type C2mQ struct {
	UID string `xml:"uid"`
	OS  string `xml:"p"` // "MAC"
}

// M2cQr answers C2mQ with the register and relay servers.
type M2cQr struct {
	Reg   *IpPort `xml:"reg"`
	Relay *IpPort `xml:"relay"`
	Log   *IpPort `xml:"log"`
	T     *IpPort `xml:"t"`
}

// C2rC registers our address with the register server and asks for the camera.
type C2rC struct {
	UID    string `xml:"uid"`
	Cli    IpPort `xml:"cli"`
	Relay  IpPort `xml:"relay"`
	CID    int32  `xml:"cid"`
	Debug  bool   `xml:"debug"`
	Family uint8  `xml:"family"` // 4
	OS     string `xml:"p"`
	R      *int32 `xml:"r"` // revision, 3
}

// R2cCr is the register server's answer: where the camera is and the session.
type R2cCr struct {
	Dev    *IpPort `xml:"dev"`   // LAN address
	Dmap   *IpPort `xml:"dmap"`  // public address, for hole punching
	Relay  *IpPort `xml:"relay"` // relay server
	Relayt *IpPort `xml:"relayt"`
	NAT    string  `xml:"nat"`
	SID    *uint32 `xml:"sid"` // missing when rsp is -3
	Rsp    int32   `xml:"rsp"` // 0 ok, -1 and -3 failure
	AC     uint32  `xml:"ac"`
}

// C2rCfm tells the register server which path was established.
type C2rCfm struct {
	SID  uint32 `xml:"sid"`
	Conn string `xml:"conn"` // local, map, relay
	Rsp  int32  `xml:"rsp"`
	CID  int32  `xml:"cid"`
	DID  int32  `xml:"did"`
}

type R2cDisc struct {
	SID uint32 `xml:"sid"`
}

// C2dC is the direct (LAN) connect for a UID.
type C2dC struct {
	UID   string `xml:"uid"`
	Cli   Port   `xml:"cli"`
	CID   int32  `xml:"cid"`
	MTU   uint32 `xml:"mtu"`
	Debug bool   `xml:"debug"`
	OS    string `xml:"p"`
}

type Timer struct {
	Def uint32 `xml:"def"`
	HB  uint32 `xml:"hb"`
	HBT uint32 `xml:"hbt"`
}

type D2cCr struct {
	Timer Timer `xml:"timer"`
	Rsp   int32 `xml:"rsp"` // 0 ok, -3 failure
	CID   int32 `xml:"cid"`
	DID   int32 `xml:"did"`
}

type C2dT struct {
	SID  uint32 `xml:"sid"`
	Conn string `xml:"conn"`
	CID  int32  `xml:"cid"`
	MTU  uint32 `xml:"mtu"`
}

type D2cT struct {
	SID  uint32 `xml:"sid"`
	Conn string `xml:"conn"`
	CID  int32  `xml:"cid"`
	DID  int32  `xml:"did"`
}

type C2dA struct {
	SID  uint32 `xml:"sid"`
	Conn string `xml:"conn"`
	CID  int32  `xml:"cid"`
	DID  int32  `xml:"did"`
	MTU  uint32 `xml:"mtu"`
}

type D2cCfm struct {
	SID   uint32  `xml:"sid"`
	Conn  string  `xml:"conn"`
	Rsp   int32   `xml:"rsp"`
	CID   int32   `xml:"cid"`
	DID   int32   `xml:"did"`
	TimeR *uint32 `xml:"time_r"`
}

type C2dHb struct {
	CID int32 `xml:"cid"`
	DID int32 `xml:"did"`
}

type D2cHb struct {
	CID int32 `xml:"cid"`
	DID int32 `xml:"did"`
}

type C2dDisc struct {
	CID int32 `xml:"cid"`
	DID int32 `xml:"did"`
}

type D2cDisc struct {
	CID int32 `xml:"cid"`
	DID int32 `xml:"did"`
}

func (*C2mQ) XMLName() string    { return "C2M_Q" }
func (*M2cQr) XMLName() string   { return "M2C_Q_R" }
func (*C2rC) XMLName() string    { return "C2R_C" }
func (*R2cCr) XMLName() string   { return "R2C_C_R" }
func (*C2rCfm) XMLName() string  { return "C2R_CFM" }
func (*R2cDisc) XMLName() string { return "R2C_DISC" }
func (*C2dC) XMLName() string    { return "C2D_C" }
func (*D2cCr) XMLName() string   { return "D2C_C_R" }
func (*C2dT) XMLName() string    { return "C2D_T" }
func (*D2cT) XMLName() string    { return "D2C_T" }
func (*C2dA) XMLName() string    { return "C2D_A" }
func (*D2cCfm) XMLName() string  { return "D2C_CFM" }
func (*C2dHb) XMLName() string   { return "C2D_HB" }
func (*D2cHb) XMLName() string   { return "D2C_HB" }
func (*C2dDisc) XMLName() string { return "C2D_DISC" }
func (*D2cDisc) XMLName() string { return "D2C_DISC" }

var messages = map[string]func() Message{}

func init() {
	for _, m := range []Message{
		&C2mQ{}, &M2cQr{}, &C2rC{}, &R2cCr{}, &C2rCfm{}, &R2cDisc{}, &C2dC{}, &D2cCr{},
		&C2dT{}, &D2cT{}, &C2dA{}, &D2cCfm{}, &C2dHb{}, &D2cHb{}, &C2dDisc{}, &D2cDisc{},
	} {
		typ := reflect.TypeOf(m).Elem()
		messages[m.XMLName()] = func() Message { return reflect.New(typ).Interface().(Message) }
	}
}

// ErrUnknownMessage is returned for P2P messages we do not use (R2C_T, ...).
var ErrUnknownMessage = errors.New("udp: unknown P2P message")

// parseXML parses a <P2P> document.
func parseXML(b []byte) (Message, error) {
	d := xml.NewDecoder(bytes.NewReader(b))

	var root bool
	for {
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if !root {
			if start.Name.Local != "P2P" {
				return nil, fmt.Errorf("root element %q", start.Name.Local)
			}
			root = true
			continue
		}

		mk := messages[start.Name.Local]
		if mk == nil {
			return nil, fmt.Errorf("%w %q", ErrUnknownMessage, start.Name.Local)
		}
		m := mk()
		if err = d.DecodeElement(m, &start); err != nil {
			return nil, err
		}
		return m, nil
	}
}

// marshalXML writes the message the way the camera does: every element on its
// own line, nested ones as blocks.
func marshalXML(m Message) []byte {
	var b bytes.Buffer
	b.WriteString("<P2P>\n")
	writeElem(&b, m.XMLName(), reflect.ValueOf(m).Elem())
	b.WriteString("</P2P>\n")
	return b.Bytes()
}

func writeElem(b *bytes.Buffer, name string, v reflect.Value) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Struct {
		b.WriteString("<" + name + ">\n")
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			writeElem(b, t.Field(i).Tag.Get("xml"), v.Field(i))
		}
		b.WriteString("</" + name + ">\n")
		return
	}

	var s string
	switch v.Kind() {
	case reflect.String:
		var esc bytes.Buffer
		_ = xml.EscapeText(&esc, []byte(v.String()))
		s = esc.String()
	case reflect.Bool:
		s = strconv.FormatBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s = strconv.FormatInt(v.Int(), 10)
	default:
		s = strconv.FormatUint(v.Uint(), 10)
	}
	b.WriteString("<" + name + ">" + s + "</" + name + ">\n")
}
