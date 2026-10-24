package onvif

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	PTZGetConfigurations       = "GetConfigurations"
	PTZGetConfiguration        = "GetConfiguration"
	PTZGetConfigurationOptions = "GetConfigurationOptions"
	PTZGetNodes                = "GetNodes"
	PTZGetNode                 = "GetNode"
	PTZGetPresets              = "GetPresets"
	PTZGotoPreset              = "GotoPreset"
	PTZSetPreset               = "SetPreset"
	PTZRemovePreset            = "RemovePreset"
	PTZContinuousMove          = "ContinuousMove"
	PTZStop                    = "Stop"
	PTZGetStatus               = "GetStatus"
)

const PathPTZ = "/onvif/ptz_service"

const (
	nsPTZ   = "http://www.onvif.org/ver20/ptz/wsdl"
	nsError = "http://www.onvif.org/ver10/error"

	spacePanTiltVelocity = "http://www.onvif.org/ver10/tptz/PanTiltSpaces/VelocityGenericSpace"
	spaceZoomVelocity    = "http://www.onvif.org/ver10/tptz/ZoomSpaces/VelocityGenericSpace"
)

// PTZFeatures is what a camera can move. Only continuous velocity is offered,
// never absolute or relative spaces, so clients do not try autotracking.
type PTZFeatures struct {
	PanTilt, Zoom bool
}

// PTZPreset is a stored position, Token is the camera's preset id.
type PTZPreset struct {
	Token, Name string
}

// ptzEnvelope is an envelope with the PTZ namespace declared in the response
// element itself, so the namespaces of the other envelopes stay as they are.
func ptzResponse(operation, attrs, inner string) []byte {
	e := NewEnvelope()
	if inner == "" {
		e.Appendf(`<tptz:%sResponse xmlns:tptz="%s"%s />`, operation, nsPTZ, attrs)
	} else {
		e.Appendf(`<tptz:%sResponse xmlns:tptz="%s"%s>%s</tptz:%sResponse>`, operation, nsPTZ, attrs, inner, operation)
	}
	return e.Bytes()
}

func appendPTZConfiguration(e *Envelope, tag, name string, f PTZFeatures) {
	e.Appendf(`<%s token="ptz_%s">`, tag, name)
	e.Appendf(`<tt:Name>PTZ</tt:Name><tt:UseCount>1</tt:UseCount><tt:NodeToken>ptz_node_%s</tt:NodeToken>`, name)
	if f.PanTilt {
		e.Appendf(`<tt:DefaultContinuousPanTiltVelocitySpace>%s</tt:DefaultContinuousPanTiltVelocitySpace>`, spacePanTiltVelocity)
	}
	if f.Zoom {
		e.Appendf(`<tt:DefaultContinuousZoomVelocitySpace>%s</tt:DefaultContinuousZoomVelocitySpace>`, spaceZoomVelocity)
	}
	e.Append(`<tt:DefaultPTZSpeed>`)
	if f.PanTilt {
		e.Append(`<tt:PanTilt x="0.5" y="0.5" />`)
	}
	if f.Zoom {
		e.Append(`<tt:Zoom x="0.5" />`)
	}
	e.Append(`</tt:DefaultPTZSpeed><tt:DefaultPTZTimeout>PT10S</tt:DefaultPTZTimeout></`, tag, `>`)
}

// PTZNamed is a stream name with its features, for the answers that list all.
type PTZNamed struct {
	Name string
	PTZFeatures
}

func ptzConfigurations(operation, tag string, list []PTZNamed) []byte {
	e := &Envelope{}
	for _, n := range list {
		appendPTZConfiguration(e, "tptz:"+tag, n.Name, n.PTZFeatures)
	}
	return ptzResponse(operation, "", string(e.buf))
}

func PTZConfigurationsResponse(name string, f PTZFeatures) []byte {
	return PTZConfigurationsResponseAll([]PTZNamed{{name, f}})
}

// PTZConfigurationsResponseAll lists the configuration of every stream in list.
func PTZConfigurationsResponseAll(list []PTZNamed) []byte {
	return ptzConfigurations("GetConfigurations", "PTZConfiguration", list)
}

func PTZConfigurationResponse(name string, f PTZFeatures) []byte {
	return ptzConfigurations("GetConfiguration", "PTZConfiguration", []PTZNamed{{name, f}})
}

func appendSpaces(e *Envelope, f PTZFeatures) {
	if f.PanTilt {
		e.Appendf(`<tt:ContinuousPanTiltVelocitySpace><tt:URI>%s</tt:URI><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange><tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange></tt:ContinuousPanTiltVelocitySpace>`, spacePanTiltVelocity)
	}
	if f.Zoom {
		e.Appendf(`<tt:ContinuousZoomVelocitySpace><tt:URI>%s</tt:URI><tt:XRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:XRange></tt:ContinuousZoomVelocitySpace>`, spaceZoomVelocity)
	}
}

func PTZConfigurationOptionsResponse(f PTZFeatures) []byte {
	e := &Envelope{}
	e.Append(`<tptz:PTZConfigurationOptions><tt:Spaces>`)
	appendSpaces(e, f)
	e.Append(`</tt:Spaces><tt:PTZTimeout><tt:Min>PT1S</tt:Min><tt:Max>PT10S</tt:Max></tt:PTZTimeout></tptz:PTZConfigurationOptions>`)
	return ptzResponse("GetConfigurationOptions", "", string(e.buf))
}

func appendNode(e *Envelope, name string, f PTZFeatures, maxPresets int) {
	e.Appendf(`<tptz:PTZNode token="ptz_node_%s" FixedHomePosition="false" GeoMove="false"><tt:Name>PTZ</tt:Name><tt:SupportedPTZSpaces>`, name)
	appendSpaces(e, f)
	e.Appendf(`</tt:SupportedPTZSpaces><tt:MaximumNumberOfPresets>%d</tt:MaximumNumberOfPresets><tt:HomeSupported>false</tt:HomeSupported></tptz:PTZNode>`, maxPresets)
}

func PTZNodesResponse(name string, f PTZFeatures, maxPresets int) []byte {
	return PTZNodesResponseAll([]PTZNamed{{name, f}}, maxPresets)
}

// PTZNodesResponseAll lists the node of every stream in list.
func PTZNodesResponseAll(list []PTZNamed, maxPresets int) []byte {
	e := &Envelope{}
	for _, n := range list {
		appendNode(e, n.Name, n.PTZFeatures, maxPresets)
	}
	return ptzResponse("GetNodes", "", string(e.buf))
}

func PTZNodeResponse(name string, f PTZFeatures, maxPresets int) []byte {
	e := &Envelope{}
	appendNode(e, name, f, maxPresets)
	return ptzResponse("GetNode", "", string(e.buf))
}

func PTZServiceCapabilitiesResponse() []byte {
	e := &Envelope{}
	e.Append(`<tptz:Capabilities EFlip="false" Reverse="false" GetCompatibleConfigurations="false" MoveStatus="false" StatusPosition="false" />`)
	return ptzResponse("GetServiceCapabilities", "", string(e.buf))
}

func PTZPresetsResponse(presets []PTZPreset) []byte {
	e := &Envelope{}
	for _, p := range presets {
		e.Appendf(`<tptz:Preset token="%s">`, escape(p.Token))
		if p.Name != "" {
			e.Appendf(`<tt:Name>%s</tt:Name>`, escape(p.Name))
		}
		e.Append(`</tptz:Preset>`)
	}
	return ptzResponse("GetPresets", "", string(e.buf))
}

func PTZSetPresetResponse(token string) []byte {
	return ptzResponse("SetPreset", "", "<tptz:PresetToken>"+escape(token)+"</tptz:PresetToken>")
}

// PTZEmptyResponse is the answer of GotoPreset, ContinuousMove and Stop.
func PTZEmptyResponse(operation string) []byte {
	return ptzResponse(operation, "", "")
}

func escape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// FaultResponse is a SOAP 1.2 fault from the receiver with an ONVIF subcode
// (ter:NoProfile, ter:NoToken...). Callers send it with HTTP 500.
func FaultResponse(subcode, reason string) []byte {
	return []byte(`<?xml version="1.0" encoding="utf-8"?><env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope" xmlns:ter="` + nsError + `"><env:Body><env:Fault><env:Code><env:Value>env:Receiver</env:Value><env:Subcode><env:Value>ter:` +
		escape(subcode) + `</env:Value></env:Subcode></env:Code><env:Reason><env:Text xml:lang="en">` +
		escape(reason) + `</env:Text></env:Reason></env:Fault></env:Body></env:Envelope>`)
}

type ContinuousMoveRequest struct {
	Token   string
	PanTilt struct{ X, Y float64 }
	Zoom    float64

	HasPanTilt, HasZoom bool

	Timeout time.Duration // 0 if the request has none
}

// fields reads the elements of a request by local name, in order, with their
// attributes, whatever the namespace prefixes are.
type field struct {
	name  string
	attrs map[string]string
	text  string
	in    []string // names of the open elements around it
}

func readFields(b []byte) ([]field, error) {
	d := xml.NewDecoder(bytes.NewReader(b))
	var out []field
	var stack []string
	cur := -1
	elements := 0
	for {
		tok, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			elements++
			f := field{name: t.Name.Local, attrs: map[string]string{}, in: append([]string(nil), stack...)}
			for _, a := range t.Attr {
				f.attrs[a.Name.Local] = a.Value
			}
			out = append(out, f)
			cur = len(out) - 1
			stack = append(stack, t.Name.Local)
		case xml.CharData:
			if cur >= 0 && len(stack) > 0 && out[cur].name == stack[len(stack)-1] {
				out[cur].text += string(t)
			}
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			cur = -1
		}
	}
	if elements == 0 {
		return nil, errors.New("onvif: no xml")
	}
	return out, nil
}

func (f field) inside(name string) bool {
	for _, n := range f.in {
		if n == name {
			return true
		}
	}
	return false
}

func text(fields []field, name string) (string, bool) {
	for _, f := range fields {
		if f.name == name {
			return strings.TrimSpace(f.text), true
		}
	}
	return "", false
}

func parseVelocity(attrs map[string]string, key string) (float64, error) {
	s, ok := attrs[key]
	if !ok {
		return 0, nil
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("onvif: bad velocity %q", s)
	}
	return v, nil
}

// ParseContinuousMove reads ProfileToken, Velocity (PanTilt x,y and Zoom x) and
// the optional ISO 8601 Timeout. NaN and out of range values are passed on as
// they are.
func ParseContinuousMove(b []byte) (r ContinuousMoveRequest, err error) {
	fields, err := readFields(b)
	if err != nil {
		return r, err
	}
	r.Token, _ = text(fields, "ProfileToken")

	for _, f := range fields {
		if !f.inside("Velocity") {
			continue
		}
		switch f.name {
		case "PanTilt":
			r.HasPanTilt = true
			if r.PanTilt.X, err = parseVelocity(f.attrs, "x"); err != nil {
				return r, err
			}
			if r.PanTilt.Y, err = parseVelocity(f.attrs, "y"); err != nil {
				return r, err
			}
		case "Zoom":
			r.HasZoom = true
			if r.Zoom, err = parseVelocity(f.attrs, "x"); err != nil {
				return r, err
			}
		}
	}
	if !r.HasPanTilt && !r.HasZoom {
		return r, errors.New("onvif: no velocity")
	}

	if s, ok := text(fields, "Timeout"); ok {
		if r.Timeout, err = parseDuration(s); err != nil {
			return r, err
		}
	}
	return r, nil
}

var reDuration = regexp.MustCompile(`^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$`)

// parseDuration reads the time part of an ISO 8601 duration: PT0.5S, PT10S, PT1M.
func parseDuration(s string) (time.Duration, error) {
	m := reDuration.FindStringSubmatch(s)
	if m == nil || s == "PT" {
		return 0, fmt.Errorf("onvif: bad duration %q", s)
	}
	var d float64
	for i, unit := range []float64{3600, 60, 1} {
		if m[i+1] == "" {
			continue
		}
		v, err := strconv.ParseFloat(m[i+1], 64)
		if err != nil {
			return 0, err
		}
		d += v * unit
	}
	if math.IsInf(d, 0) || d > math.MaxInt64/float64(time.Second) {
		return 0, fmt.Errorf("onvif: duration %q too long", s)
	}
	return time.Duration(math.Round(d * float64(time.Second))), nil
}

// ParseStop reads the ProfileToken and the PanTilt and Zoom flags, which are
// true if absent (or if there is nothing to read).
func ParseStop(b []byte) (token string, panTilt, zoom bool) {
	panTilt, zoom = true, true
	fields, err := readFields(b)
	if err != nil {
		return
	}
	token, _ = text(fields, "ProfileToken")
	flag := func(name string, v *bool) {
		if s, ok := text(fields, name); ok && s != "" {
			*v = s == "true" || s == "1"
		}
	}
	flag("PanTilt", &panTilt)
	flag("Zoom", &zoom)
	return
}

// ParsePreset reads ProfileToken, PresetToken and PresetName.
func ParsePreset(b []byte) (token, presetToken, presetName string) {
	fields, err := readFields(b)
	if err != nil {
		return
	}
	token, _ = text(fields, "ProfileToken")
	presetToken, _ = text(fields, "PresetToken")
	presetName, _ = text(fields, "PresetName")
	return
}
