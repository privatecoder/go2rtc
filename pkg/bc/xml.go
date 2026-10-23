package bc

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
)

// XMLVersion is the version attribute used on every element we send.
const XMLVersion = "1.1"

const xmlDecl = `<?xml version="1.0" encoding="UTF-8" ?>` + "\n"

// Body is the XML payload of a message (root element <body>). Only the
// elements go2rtc needs are mapped, anything else is ignored on parse.
type Body struct {
	XMLName     xml.Name     `xml:"body"`
	Encryption  *Encryption  `xml:"Encryption,omitempty"`
	LoginUser   *LoginUser   `xml:"LoginUser,omitempty"`
	LoginNet    *LoginNet    `xml:"LoginNet,omitempty"`
	DeviceInfo  *DeviceInfo  `xml:"DeviceInfo,omitempty"`
	Preview     *Preview     `xml:"Preview,omitempty"`
	TalkConfig  *TalkConfig  `xml:"TalkConfig,omitempty"`
	TalkAbility *TalkAbility `xml:"TalkAbility,omitempty"`
	LinkType    *LinkType    `xml:"LinkType,omitempty"`
}

// LinkType is the reply to a ping (msg 93).
type LinkType struct {
	Type string `xml:"type"` // "LAN"
}

type Encryption struct {
	Version string `xml:"version,attr"`
	Type    string `xml:"type"` // only "md5" seen
	Nonce   string `xml:"nonce"`
}

type LoginUser struct {
	Version  string `xml:"version,attr"`
	UserName string `xml:"userName"`
	Password string `xml:"password"`
	UserVer  uint32 `xml:"userVer"` // always 1
}

type LoginNet struct {
	Version string `xml:"version,attr"`
	Type    string `xml:"type"`    // "LAN"
	UDPPort uint16 `xml:"udpPort"` // 0 for TCP
}

// DeviceInfo is deliberately lenient: the real element has dozens of fields.
type DeviceInfo struct {
	Version     string      `xml:"version,attr,omitempty"`
	Type        string      `xml:"type,omitempty"`        // ipc, nvr...
	TypeInfo    string      `xml:"typeInfo,omitempty"`    // model class
	FirmVersion string      `xml:"firmVersion,omitempty"` // often zeros, not the real firmware
	Resolution  *Resolution `xml:"resolution,omitempty"`  // absent on floodlights
}

type Resolution struct {
	Name   string `xml:"resolutionName"` // "2304*1296"
	Width  uint32 `xml:"width"`
	Height uint32 `xml:"height"`
}

// Preview requests a stream (msg 3).
type Preview struct {
	Version    string `xml:"version,attr"`
	ChannelID  uint8  `xml:"channelId"`
	Handle     uint32 `xml:"handle"`
	StreamType string `xml:"streamType,omitempty"` // mainStream | subStream | externStream
}

type TalkConfig struct {
	Version         string      `xml:"version,attr"`
	ChannelID       uint8       `xml:"channelId"`
	Duplex          string      `xml:"duplex"`          // "FDX"
	AudioStreamMode string      `xml:"audioStreamMode"` // "followVideoStream"
	AudioConfig     AudioConfig `xml:"audioConfig"`
}

type AudioConfig struct {
	Priority         *uint32 `xml:"priority,omitempty"` // only in TalkAbility replies
	AudioType        string  `xml:"audioType"`          // "adpcm"
	SampleRate       uint16  `xml:"sampleRate"`
	SamplePrecision  uint16  `xml:"samplePrecision"`  // 16
	LengthPerEncoder uint16  `xml:"lengthPerEncoder"` // samples per block, 2x data bytes
	SoundTrack       string  `xml:"soundTrack"`       // "mono"
}

type TalkAbility struct {
	Version             string                `xml:"version,attr"`
	DuplexList          []DuplexList          `xml:"duplexList"`
	AudioStreamModeList []AudioStreamModeList `xml:"audioStreamModeList"`
	AudioConfigList     []AudioConfigList     `xml:"audioConfigList"`
}

type DuplexList struct {
	Duplex string `xml:"duplex"`
}

type AudioStreamModeList struct {
	AudioStreamMode []string `xml:"audioStreamMode"`
}

type AudioConfigList struct {
	AudioConfig AudioConfig `xml:"audioConfig"`
}

// Extension describes the payload that follows it (root element <Extension>).
type Extension struct {
	XMLName    xml.Name `xml:"Extension"`
	Version    string   `xml:"version,attr"`
	BinaryData *uint32  `xml:"binaryData,omitempty"`
	UserName   string   `xml:"userName,omitempty"`
	Token      string   `xml:"token,omitempty"`
	ChannelID  *uint8   `xml:"channelId,omitempty"`
	RfID       *uint8   `xml:"rfId,omitempty"`
	CheckPos   *uint32  `xml:"checkPos,omitempty"`
	CheckValue *int32   `xml:"checkValue,omitempty"`
	EncryptLen *uint32  `xml:"encryptLen,omitempty"`
}

// NewExtension returns an Extension with only the version set.
func NewExtension() *Extension {
	return &Extension{Version: XMLVersion}
}

// ParseBody parses a <body> payload.
func ParseBody(b []byte) (*Body, error) {
	v := new(Body)
	if err := xml.Unmarshal(b, v); err != nil {
		return nil, err
	}
	return v, nil
}

// ParseExtension parses an <Extension> block.
func ParseExtension(b []byte) (*Extension, error) {
	v := new(Extension)
	if err := xml.Unmarshal(b, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (b *Body) Marshal() ([]byte, error) { return marshalXML(b) }

func (e *Extension) Marshal() ([]byte, error) { return marshalXML(e) }

func marshalXML(v any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(xmlDecl)
	if err := xml.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// describe is a line for the log: the device type, firmware if the camera
// reports a real one, and the main resolution.
func (d *DeviceInfo) describe() string {
	var parts []string
	if d.TypeInfo != "" {
		parts = append(parts, d.TypeInfo)
	} else if d.Type != "" {
		parts = append(parts, d.Type)
	}
	if fw := strings.Trim(d.FirmVersion, "0"); fw != "" {
		parts = append(parts, "firmware "+d.FirmVersion)
	}
	if d.Resolution != nil {
		parts = append(parts, fmt.Sprintf("%dx%d", d.Resolution.Width, d.Resolution.Height))
	}
	if len(parts) == 0 {
		return "camera"
	}
	return strings.Join(parts, ", ")
}
