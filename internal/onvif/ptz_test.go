package onvif

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/bc"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// GetCapabilities and GetServices of the code before PTZ, host 192.168.1.50:1984.
const goldenCapabilities = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><tds:GetCapabilitiesResponse>\n\t<tds:Capabilities>\n\t\t<tt:Device>\n\t\t\t<tt:XAddr>http://192.168.1.50:1984/onvif/device_service</tt:XAddr>\n\t\t</tt:Device>\n\t\t<tt:Media>\n\t\t\t<tt:XAddr>http://192.168.1.50:1984/onvif/media_service</tt:XAddr>\n\t\t\t<tt:StreamingCapabilities>\n\t\t\t\t<tt:RTPMulticast>false</tt:RTPMulticast>\n\t\t\t\t<tt:RTP_TCP>false</tt:RTP_TCP>\n\t\t\t\t<tt:RTP_RTSP_TCP>true</tt:RTP_RTSP_TCP>\n\t\t\t</tt:StreamingCapabilities>\n\t\t</tt:Media>\n\t</tds:Capabilities>\n</tds:GetCapabilitiesResponse></s:Body></s:Envelope>"
const goldenServices = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><tds:GetServicesResponse>\n\t<tds:Service>\n\t\t<tds:Namespace>http://www.onvif.org/ver10/device/wsdl</tds:Namespace>\n\t\t<tds:XAddr>http://192.168.1.50:1984/onvif/device_service</tds:XAddr>\n\t\t<tds:Version><tt:Major>2</tt:Major><tt:Minor>5</tt:Minor></tds:Version>\n\t</tds:Service>\n\t<tds:Service>\n\t\t<tds:Namespace>http://www.onvif.org/ver10/media/wsdl</tds:Namespace>\n\t\t<tds:XAddr>http://192.168.1.50:1984/onvif/media_service</tds:XAddr>\n\t\t<tds:Version><tt:Major>2</tt:Major><tt:Minor>5</tt:Minor></tds:Version>\n\t</tds:Service>\n</tds:GetServicesResponse></s:Body></s:Envelope>"

func init() { log = zerolog.Nop() }

const testHost = "192.168.1.50:1984"

// stubPTZ is the controller of a camera: it records what it is asked to do.
type stubPTZ struct {
	mu      sync.Mutex
	caps    bc.PTZCaps
	gate    chan struct{} // Caps waits for it to close, if set
	presets []bc.Preset
	events  []string
	stopErr error
}

func (s *stubPTZ) Caps(ctx context.Context) (bc.PTZCaps, error) {
	s.mu.Lock()
	gate := s.gate
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return bc.PTZCaps{}, ctx.Err()
		}
	}
	return s.caps, nil
}

func (s *stubPTZ) setGate(g chan struct{}) {
	s.mu.Lock()
	s.gate = g
	s.mu.Unlock()
}

func (s *stubPTZ) record(e string) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

func (s *stubPTZ) Move(v bc.Velocity, timeout time.Duration) error {
	s.record("move:" + strconv.FormatFloat(v.Pan, 'g', -1, 64) + "," + strconv.FormatFloat(v.Tilt, 'g', -1, 64) +
		",pt=" + strconv.FormatBool(v.HasPanTilt) + ",zoom=" + strconv.FormatFloat(v.Zoom, 'g', -1, 64) + ",z=" + strconv.FormatBool(v.HasZoom) +
		",timeout=" + timeout.String())
	return nil
}

func (s *stubPTZ) Stop(panTilt, zoom bool) error {
	s.record("stop:" + strconv.FormatBool(panTilt) + "," + strconv.FormatBool(zoom))
	return s.stopErr
}

func (s *stubPTZ) Presets() ([]bc.Preset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bc.Preset(nil), s.presets...), nil
}

func (s *stubPTZ) GotoPreset(id uint8) error {
	s.record("goto:" + strconv.Itoa(int(id)))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.presets {
		if p.ID == id {
			return nil
		}
	}
	return errors.New("bc: msg 19: camera answered 400") // the camera does not know it
}

func (s *stubPTZ) SetPreset(id int, name string) (uint8, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id < 0 {
		id = 0
		for {
			used := false
			for _, p := range s.presets {
				used = used || int(p.ID) == id
			}
			if !used {
				break
			}
			id++
		}
	}
	s.presets = append(s.presets, bc.Preset{ID: uint8(id), Name: name})
	s.events = append(s.events, "set:"+strconv.Itoa(id)+":"+name)
	return uint8(id), nil
}

func (s *stubPTZ) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// setup registers streams and the cameras behind their bc sources.
func setup(t *testing.T, cams map[string]*stubPTZ, streamSources map[string][]string) {
	t.Helper()
	for _, scheme := range []string{"bc", "rtsp", "ffmpeg"} {
		streams.HandleFunc(scheme, func(string) (core.Producer, error) { return nil, errors.New("test") })
	}

	for name, sources := range streamSources {
		_, err := streams.New(name, sources...)
		require.NoError(t, err)
		t.Cleanup(func() { streams.Delete(name) })
	}

	resetCamStates()
	t.Cleanup(resetCamStates)

	old := ptzFor
	ptzFor = func(rawURL string) (ptzController, error) {
		if c, ok := cams[rawURL]; ok {
			return c, nil
		}
		return nil, errors.New("no such camera")
	}
	t.Cleanup(func() { ptzFor = old })
}

const camURL = "bc://user:pass@192.168.1.50?channel=0"

func soap(inner string) string {
	return `<?xml version='1.0' encoding='utf-8'?><soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope"><soap-env:Body>` + inner + `</soap-env:Body></soap-env:Envelope>`
}

func ptzBody(op, inner string) string {
	return soap(`<ns0:` + op + ` xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl">` + inner + `</ns0:` + op + `>`)
}

func call(t *testing.T, path, body string) (int, string, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "http://"+testHost+path, strings.NewReader(body))
	w := httptest.NewRecorder()
	onvifDeviceService(w, req)
	res := w.Result()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header.Get("Content-Type"), string(b)
}

func callPTZ(t *testing.T, op, inner string) (int, string) {
	t.Helper()
	code, ct, b := call(t, "/onvif/ptz_service", ptzBody(op, inner))
	if code != 200 {
		require.Contains(t, ct, "soap+xml", "a fault is SOAP, not plain text: %s", b)
		require.Contains(t, b, "env:Fault")
	}
	return code, b
}

func faultCode(t *testing.T, b string) string {
	var v struct {
		Sub string `xml:"Body>Fault>Code>Subcode>Value"`
	}
	require.NoError(t, xml.Unmarshal([]byte(b), &v), b)
	return v.Sub
}

func setVar[T any](t *testing.T, v *T, val T) {
	old := *v
	*v = val
	t.Cleanup(func() { *v = old })
}

func zoomCam() *stubPTZ {
	return &stubPTZ{caps: bc.PTZCaps{PanTilt: true, Zoom: true, ZoomMax: 33}}
}

func TestBCSource(t *testing.T) {
	setup(t, nil, map[string][]string{
		"first":  {camURL, "ffmpeg:x"},
		"second": {"ffmpeg:x#audio=opus", "bc://u:p@192.168.1.51"},
		"none":   {"rtsp://192.168.1.60/s"},
	})
	require.Equal(t, camURL, bcSource("first"))
	require.Equal(t, "bc://u:p@192.168.1.51", bcSource("second"))
	require.Equal(t, "", bcSource("none"))
	require.Equal(t, "", bcSource("unknown"))
	require.True(t, hasBCStreams())
}

func TestPTZProfileAdvertised(t *testing.T) {
	setup(t, map[string]*stubPTZ{camURL: zoomCam()}, map[string][]string{
		"cam":   {"ffmpeg:x#audio=opus", camURL}, // the bc source is not the first
		"other": {"rtsp://192.168.1.60/s"},
	})

	code, _, b := call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.Equal(t, 200, code)

	var v struct {
		Profiles []struct {
			Token string `xml:"token,attr"`
			PTZ   *struct {
				Token string `xml:"token,attr"`
				Pan   string `xml:"DefaultContinuousPanTiltVelocitySpace"`
				Zoom  string `xml:"DefaultContinuousZoomVelocitySpace"`
			} `xml:"PTZConfiguration"`
		} `xml:"Body>GetProfilesResponse>Profiles"`
	}
	require.NoError(t, xml.Unmarshal([]byte(b), &v))
	require.Len(t, v.Profiles, 2)
	for _, p := range v.Profiles {
		switch p.Token {
		case "cam":
			require.NotNil(t, p.PTZ)
			require.Equal(t, "ptz_cam", p.PTZ.Token)
			require.NotEmpty(t, p.PTZ.Pan)
			require.NotEmpty(t, p.PTZ.Zoom)
		case "other":
			require.Nil(t, p.PTZ)
		default:
			t.Fatal(p.Token)
		}
	}

	// the single profile too
	_, _, b = call(t, "/onvif/media_service", soap(`<trt:GetProfile xmlns:trt="http://www.onvif.org/ver10/media/wsdl"><trt:ProfileToken>cam</trt:ProfileToken></trt:GetProfile>`))
	require.Contains(t, b, `token="ptz_cam"`)
	_, _, b = call(t, "/onvif/media_service", soap(`<trt:GetProfile xmlns:trt="http://www.onvif.org/ver10/media/wsdl"><trt:ProfileToken>other</trt:ProfileToken></trt:GetProfile>`))
	require.NotContains(t, b, "PTZConfiguration")

	// the device offers the PTZ service
	_, _, b = call(t, "/onvif/device_service", soap(`<tds:GetCapabilities xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>`))
	require.Contains(t, b, "http://"+testHost+"/onvif/ptz_service")
	_, _, b = call(t, "/onvif/device_service", soap(`<tds:GetServices xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>`))
	require.Contains(t, b, "http://www.onvif.org/ver20/ptz/wsdl")
}

func TestPTZProfileWithoutPTZCamera(t *testing.T) {
	setup(t, map[string]*stubPTZ{camURL: {}}, map[string][]string{"cam": {camURL}})
	_, _, b := call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.Contains(t, b, `token="cam"`)
	require.NotContains(t, b, "PTZConfiguration")
}

func TestPTZZoomlessCamera(t *testing.T) {
	setup(t, map[string]*stubPTZ{camURL: {caps: bc.PTZCaps{PanTilt: true}}}, map[string][]string{"cam": {camURL}})
	_, _, b := call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.Contains(t, b, "DefaultContinuousPanTiltVelocitySpace")
	require.NotContains(t, b, "DefaultContinuousZoomVelocitySpace")
}

func TestPTZContinuousMoveAndStop(t *testing.T) {
	cam := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})

	code, b := callPTZ(t, "ContinuousMove", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="http://www.onvif.org/ver10/schema" x="1" y="0"/></ns0:Velocity>`)
	require.Equal(t, 200, code, b)
	require.Contains(t, b, "ContinuousMoveResponse")

	code, b = callPTZ(t, "ContinuousMove", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:Zoom xmlns:ns1="http://www.onvif.org/ver10/schema" x="-1"/></ns0:Velocity><ns0:Timeout>PT2S</ns0:Timeout>`)
	require.Equal(t, 200, code, b)

	code, b = callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PanTilt>true</ns0:PanTilt><ns0:Zoom>true</ns0:Zoom>`)
	require.Equal(t, 200, code, b)
	require.Contains(t, b, "StopResponse")

	code, _ = callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
	require.Equal(t, 200, code)

	require.Equal(t, []string{
		"move:1,0,pt=true,zoom=0,z=false,timeout=0s",
		"move:0,0,pt=false,zoom=-1,z=true,timeout=2s",
		"stop:true,true",
		"stop:true,true",
	}, cam.log())
}

func TestPTZTiltSignAndNaN(t *testing.T) {
	cam := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})
	// +y is up in ONVIF, and the controller takes it as it is, odd values included
	code, _ := callPTZ(t, "ContinuousMove", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="x" x="NaN" y="0.5"/></ns0:Velocity>`)
	require.Equal(t, 200, code)
	require.Equal(t, []string{"move:NaN,0.5,pt=true,zoom=0,z=false,timeout=0s"}, cam.log())
}

func TestPTZBadRequestsAreFaultsAndMoveNothing(t *testing.T) {
	cam := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}, "other": {"rtsp://192.168.1.60/s"}})

	for name, inner := range map[string]string{
		"no velocity": `<ns0:ProfileToken>cam</ns0:ProfileToken>`,
		"bad number":  `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="x" x="fast" y="0"/></ns0:Velocity>`,
		"bad timeout": `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:Zoom xmlns:ns1="x" x="1"/></ns0:Velocity><ns0:Timeout>soon</ns0:Timeout>`,
		"empty":       ``,
	} {
		code, b := callPTZ(t, "ContinuousMove", inner)
		require.Equal(t, 500, code, name)
		require.Contains(t, []string{"InvalidArgVal", "NoProfile"}, strings.TrimPrefix(faultCode(t, b), "ter:"), name)
	}

	// unknown profile, and one without a bc source
	for _, token := range []string{"nope", "other", ""} {
		code, b := callPTZ(t, "ContinuousMove", `<ns0:ProfileToken>`+token+`</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="x" x="1" y="0"/></ns0:Velocity>`)
		require.Equal(t, 500, code)
		require.Equal(t, "ter:NoProfile", faultCode(t, b), token)

		code, b = callPTZ(t, "Stop", `<ns0:ProfileToken>`+token+`</ns0:ProfileToken>`)
		require.Equal(t, 500, code)
		require.Equal(t, "ter:NoProfile", faultCode(t, b))

		code, b = callPTZ(t, "GetPresets", `<ns0:ProfileToken>`+token+`</ns0:ProfileToken>`)
		require.Equal(t, 500, code)
		require.Equal(t, "ter:NoProfile", faultCode(t, b))
	}
	require.Empty(t, cam.log())
}

func TestPTZStopErrorIsFault(t *testing.T) {
	cam := zoomCam()
	cam.stopErr = errors.New("bc: ptz stop not acknowledged yet, retrying")
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})
	code, b := callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
	require.Equal(t, 500, code)
	require.Contains(t, b, "camera not reachable")
	require.NotContains(t, b, "not acknowledged") // the controller's words stay in the log
}

func TestPTZPresets(t *testing.T) {
	cam := zoomCam()
	cam.presets = []bc.Preset{{ID: 0, Name: "home"}}
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})

	code, b := callPTZ(t, "SetPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetName>door</ns0:PresetName>`)
	require.Equal(t, 200, code, b)
	var set struct {
		Token string `xml:"Body>SetPresetResponse>PresetToken"`
	}
	require.NoError(t, xml.Unmarshal([]byte(b), &set))
	require.Equal(t, "1", set.Token) // the lowest free slot

	code, b = callPTZ(t, "GetPresets", `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
	require.Equal(t, 200, code)
	var list struct {
		Presets []struct {
			Token string `xml:"token,attr"`
			Name  string `xml:"Name"`
		} `xml:"Body>GetPresetsResponse>Preset"`
	}
	require.NoError(t, xml.Unmarshal([]byte(b), &list))
	require.Len(t, list.Presets, 2)
	require.Equal(t, "home", list.Presets[0].Name)
	require.Equal(t, "1", list.Presets[1].Token)
	require.Equal(t, "door", list.Presets[1].Name)

	code, b = callPTZ(t, "GotoPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetToken>1</ns0:PresetToken>`)
	require.Equal(t, 200, code, b)

	// overwrite a given slot
	code, b = callPTZ(t, "SetPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetName>porch</ns0:PresetName><ns0:PresetToken>5</ns0:PresetToken>`)
	require.Equal(t, 200, code, b)
	require.Contains(t, b, "<tptz:PresetToken>5</tptz:PresetToken>")

	// not a preset id: refused before the camera is asked
	for _, token := range []string{"x", "-1", "300", ""} {
		code, b = callPTZ(t, "GotoPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetToken>`+token+`</ns0:PresetToken>`)
		require.Equal(t, 500, code, token)
		require.Equal(t, "ter:NoToken", faultCode(t, b), token)
	}
	require.NotContains(t, strings.Join(cam.log(), " "), "goto:99")

	// an id the camera refuses: its error, not worded for the client
	code, b = callPTZ(t, "GotoPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetToken>99</ns0:PresetToken>`)
	require.Equal(t, 500, code)
	require.Equal(t, "ter:Action", faultCode(t, b))
	require.Contains(t, b, "camera not reachable")
	require.NotContains(t, b, "400")
	code, b = callPTZ(t, "SetPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetName>x</ns0:PresetName><ns0:PresetToken>abc</ns0:PresetToken>`)
	require.Equal(t, 500, code)
	require.Equal(t, "ter:NoToken", faultCode(t, b))

	require.Equal(t, []string{"set:1:door", "goto:1", "set:5:porch", "goto:99"}, cam.log())
}

func TestPTZUnsupportedOperationsAreFaults(t *testing.T) {
	setup(t, map[string]*stubPTZ{camURL: zoomCam()}, map[string][]string{"cam": {camURL}})
	for _, op := range []string{"RemovePreset", "GetStatus", "AbsoluteMove", "RelativeMove", "GotoHomePosition", "Nonsense"} {
		code, b := callPTZ(t, op, `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
		require.Equal(t, 500, code, op)
		require.Equal(t, "ter:ActionNotSupported", faultCode(t, b), op)
	}

	// no body at all: a fault as well, on the PTZ path
	code, ct, b := call(t, "/onvif/ptz_service", "")
	require.Equal(t, 500, code)
	require.Contains(t, ct, "soap+xml")
	require.Contains(t, b, "env:Fault")
}

func TestPTZDescriptions(t *testing.T) {
	setup(t, map[string]*stubPTZ{camURL: zoomCam()}, map[string][]string{"cam": {camURL}, "other": {"rtsp://192.168.1.60/s"}})

	code, b := callPTZ(t, "GetServiceCapabilities", ``)
	require.Equal(t, 200, code)
	require.Contains(t, b, `MoveStatus="false"`)

	code, b = callPTZ(t, "GetConfigurations", ``)
	require.Equal(t, 200, code)
	require.Contains(t, b, `token="ptz_cam"`)
	require.NotContains(t, b, "other")

	code, b = callPTZ(t, "GetConfiguration", `<ns0:PTZConfigurationToken>ptz_cam</ns0:PTZConfigurationToken>`)
	require.Equal(t, 200, code)
	require.Contains(t, b, `token="ptz_cam"`)

	code, b = callPTZ(t, "GetConfigurationOptions", `<ns0:ConfigurationToken>ptz_cam</ns0:ConfigurationToken>`)
	require.Equal(t, 200, code)
	require.Contains(t, b, "ContinuousPanTiltVelocitySpace")
	require.NotContains(t, b, "Relative")

	code, b = callPTZ(t, "GetNodes", ``)
	require.Equal(t, 200, code)
	require.Contains(t, b, `token="ptz_node_cam"`)

	code, b = callPTZ(t, "GetNode", `<ns0:NodeToken>ptz_node_cam</ns0:NodeToken>`)
	require.Equal(t, 200, code)
	require.Contains(t, b, `token="ptz_node_cam"`)

	for _, tc := range [][2]string{
		{"GetConfiguration", `<ns0:PTZConfigurationToken>ptz_other</ns0:PTZConfigurationToken>`},
		{"GetConfigurationOptions", `<ns0:ConfigurationToken>ptz_nope</ns0:ConfigurationToken>`},
		{"GetNode", `<ns0:NodeToken>ptz_node_other</ns0:NodeToken>`},
	} {
		code, b = callPTZ(t, tc[0], tc[1])
		require.Equal(t, 500, code, tc[0])
		require.Contains(t, []string{"ter:NoConfig", "ter:NoEntity"}, faultCode(t, b), tc[0])
	}
}

func TestPTZNoBCStreams(t *testing.T) {
	setup(t, nil, map[string][]string{"a": {"rtsp://192.168.1.60/s"}, "b": {"ffmpeg:x"}})
	require.False(t, hasBCStreams())

	code, _, b := call(t, "/onvif/device_service", soap(`<tds:GetCapabilities xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>`))
	require.Equal(t, 200, code)
	require.Equal(t, goldenCapabilities, b)
	code, _, b = call(t, "/onvif/device_service", soap(`<tds:GetServices xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>`))
	require.Equal(t, 200, code)
	require.Equal(t, goldenServices, b)

	_, _, b = call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.NotContains(t, b, "PTZ")
}

func TestPTZProfilesWaitBounded(t *testing.T) {
	// a camera that never answers: the profiles come in time, without PTZ
	hung := zoomCam()
	gate := make(chan struct{})
	hung.setGate(gate)
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	setup(t, map[string]*stubPTZ{camURL: hung}, map[string][]string{"cam": {camURL}})

	old := profilesWait
	profilesWait = 200 * time.Millisecond
	t.Cleanup(func() { profilesWait = old })

	start := time.Now()
	code, _, b := call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.Equal(t, 200, code)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Contains(t, b, `token="cam"`)
	require.NotContains(t, b, "PTZConfiguration")

	// when the camera answers later, the next GetProfiles has PTZ
	close(gate)
	_, _, b = call(t, "/onvif/media_service", soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`))
	require.Contains(t, b, "PTZConfiguration")
}

func TestProfilesWaitDefault(t *testing.T) {
	require.Equal(t, 8*time.Second, profilesWait)
}

func TestPTZFeaturesSharedDeadline(t *testing.T) {
	fast := zoomCam()
	slow := zoomCam()
	gate := make(chan struct{})
	slow.setGate(gate)
	t.Cleanup(func() { close(gate) })
	setup(t, map[string]*stubPTZ{camURL: fast, "bc://u:p@192.168.1.51": slow},
		map[string][]string{"fast": {camURL}, "slow": {"bc://u:p@192.168.1.51"}, "plain": {"rtsp://192.168.1.60/s"}})

	start := time.Now()
	got := ptzFeatures([]string{"fast", "slow", "plain", "missing"}, 150*time.Millisecond)
	require.Less(t, time.Since(start), time.Second)
	require.Len(t, got, 1)
	require.True(t, got["fast"].PanTilt && got["fast"].Zoom)
}

func TestPTZFaultDoesNotDiscloseCameraAddress(t *testing.T) {
	c := zoomCam()
	c.stopErr = errors.New("udp: camera 95270000ABCDEFGH not reachable: dial tcp 192.168.1.50:9000: refused")
	setup(t, map[string]*stubPTZ{camURL: c}, map[string][]string{"cam": {camURL}})
	code, b := callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
	require.Equal(t, 500, code)
	require.NotContains(t, b, "95270000ABCDEFGH")
	require.NotContains(t, b, "192.168.1.50")
	require.Equal(t, "ter:Action", faultCode(t, b))
}

// capsCounter is a camera that never answers its abilities in time.
type capsCounter struct {
	*stubPTZ
	calls atomic.Int32
}

func (c *capsCounter) Caps(ctx context.Context) (bc.PTZCaps, error) {
	c.calls.Add(1)
	<-ctx.Done()
	return bc.PTZCaps{}, ctx.Err()
}

func TestPTZFailedCapabilitiesDoNotDelayEveryPoll(t *testing.T) {
	c := &capsCounter{stubPTZ: zoomCam()}
	setup(t, nil, map[string][]string{"offline": {camURL}, "plain": {"rtsp://192.168.1.60/s"}})
	ptzFor = func(string) (ptzController, error) { return c, nil }
	setVar(t, &profilesWait, 35*time.Millisecond)
	setVar(t, &capsFetchTimeout, 50*time.Millisecond) // the fetch fails soon after the first poll

	request := soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`)
	call(t, "/onvif/media_service", request)
	require.Eventually(t, func() bool {
		st := stateOf(camURL)
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.pending == nil && !st.until.IsZero()
	}, time.Second, time.Millisecond)

	for i := 0; i < 3; i++ {
		start := time.Now()
		code, _, b := call(t, "/onvif/media_service", request)
		require.Equal(t, 200, code)
		require.Less(t, time.Since(start), 20*time.Millisecond, "a failed camera is not waited for")
		require.Contains(t, b, `token="offline"`) // the ordinary profiles are there
		require.Contains(t, b, `token="plain"`)
		require.NotContains(t, b, "PTZConfiguration")
	}
	require.EqualValues(t, 1, c.calls.Load(), "the failed camera was asked again inside its backoff")
}

func TestPTZSlowCapabilitiesAreNotAFailure(t *testing.T) {
	// still pending at the deadline: no new request while it runs, no backoff
	c := &capsCounter{stubPTZ: zoomCam()}
	setup(t, nil, map[string][]string{"slow": {camURL}})
	ptzFor = func(string) (ptzController, error) { return c, nil }
	setVar(t, &profilesWait, 30*time.Millisecond)

	request := soap(`<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>`)
	for i := 0; i < 3; i++ {
		start := time.Now()
		call(t, "/onvif/media_service", request)
		require.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond, "a pending fetch is waited for the usual time")
	}
	require.EqualValues(t, 1, c.calls.Load())
	st := stateOf(camURL)
	st.mu.Lock()
	defer st.mu.Unlock()
	require.True(t, st.until.IsZero())
}

func TestPTZCapsBackoffGrowsAndResets(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var calls atomic.Int32
	cam := zoomCam()
	setup(t, nil, map[string][]string{"a": {camURL}, "a-sub": {"bc://u:p@192.168.1.50?channel=0&stream=sub"}})
	ptzFor = func(string) (ptzController, error) {
		return &funcCaps{stubPTZ: cam, caps: func() (bc.PTZCaps, error) {
			calls.Add(1)
			if fail.Load() {
				return bc.PTZCaps{}, errors.New("offline")
			}
			return cam.caps, nil
		}}, nil
	}
	setVar(t, &capsBackoffMin, 40*time.Millisecond)
	setVar(t, &capsBackoffMax, 100*time.Millisecond)

	// both streams are the same camera: one fetch for the pair
	got := ptzFeatures([]string{"a", "a-sub"}, time.Second)
	require.Empty(t, got)
	require.EqualValues(t, 1, calls.Load())

	st := stateOf(camURL)
	backoff := func() time.Duration { st.mu.Lock(); defer st.mu.Unlock(); return st.backoff }
	require.Equal(t, 40*time.Millisecond, backoff())

	require.Empty(t, ptzFeatures([]string{"a"}, time.Second)) // inside the backoff
	require.EqualValues(t, 1, calls.Load())

	time.Sleep(60 * time.Millisecond)
	require.Empty(t, ptzFeatures([]string{"a"}, time.Second)) // asked again, fails again
	require.EqualValues(t, 2, calls.Load())
	require.Equal(t, 80*time.Millisecond, backoff())

	time.Sleep(100 * time.Millisecond)
	require.Empty(t, ptzFeatures([]string{"a"}, time.Second))
	require.Equal(t, 100*time.Millisecond, backoff(), "capped")

	fail.Store(false)
	time.Sleep(120 * time.Millisecond)
	got = ptzFeatures([]string{"a", "a-sub"}, time.Second)
	require.Len(t, got, 2)
	require.Equal(t, time.Duration(0), backoff())
	n := calls.Load()
	ptzFeatures([]string{"a"}, time.Second)
	require.Equal(t, n, calls.Load(), "known abilities are not asked again")
}

type funcCaps struct {
	*stubPTZ
	caps func() (bc.PTZCaps, error)
}

func (f *funcCaps) Caps(context.Context) (bc.PTZCaps, error) { return f.caps() }

// A request must reach the controller in the order it came: GotoPreset does not
// first read the camera's list, which a Stop could overtake.
func TestPTZGotoPresetHasNoPreflight(t *testing.T) {
	base := zoomCam()
	base.presets = []bc.Preset{{ID: 3, Name: "door"}}
	c := &presetsCounter{stubPTZ: base}
	setup(t, nil, map[string][]string{"cam": {camURL}})
	ptzFor = func(string) (ptzController, error) { return c, nil }

	code, b := callPTZ(t, "GotoPreset", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetToken>3</ns0:PresetToken>`)
	require.Equal(t, 200, code, b)
	code, _ = callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken>`)
	require.Equal(t, 200, code)
	require.Equal(t, []string{"goto:3", "stop:true,true"}, base.log())
	require.EqualValues(t, 0, c.lists.Load(), "the camera's preset list was read before the preset was sent")
}

type presetsCounter struct {
	*stubPTZ
	lists atomic.Int32
}

func (c *presetsCounter) Presets() ([]bc.Preset, error) {
	c.lists.Add(1)
	return c.stubPTZ.Presets()
}

func TestPTZBadStopFlagsAreFaults(t *testing.T) {
	c := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: c}, map[string][]string{"cam": {camURL}})
	code, b := callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PanTilt>tru</ns0:PanTilt><ns0:Zoom>yes</ns0:Zoom>`)
	require.Equal(t, 500, code)
	require.Equal(t, "ter:InvalidArgVal", faultCode(t, b))
	require.Empty(t, c.log())

	for _, ok := range []string{"true", "false", "1", "0"} {
		code, _ = callPTZ(t, "Stop", `<ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PanTilt>`+ok+`</ns0:PanTilt>`)
		require.Equal(t, 200, code, ok)
	}
}

// A camera that closes the connection: the profile polls open one control
// connection, not one each.
var realLoginRuns atomic.Int32

func TestPTZFailedProfilesDoNotRepeatControlLogin(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	var attempts atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			attempts.Add(1)
			_ = conn.Close()
		}
	}()
	// pkg/bc keeps one controller per host and channel for the process, and
	// its key has no port: each run uses a channel of its own
	source := "bc://review:example@" + ln.Addr().String() + "?channel=" + strconv.Itoa(int(250-realLoginRuns.Add(1)))
	realFor := ptzFor
	setup(t, nil, map[string][]string{"offline-real": {source}})
	ptzFor = realFor

	for i := 0; i < 3; i++ {
		code, _, _ := call(t, "/onvif/media_service", soap(`<trt:GetProfile xmlns:trt="http://www.onvif.org/ver10/media/wsdl"><trt:ProfileToken>offline-real</trt:ProfileToken></trt:GetProfile>`))
		require.Equal(t, 200, code)
	}
	require.EqualValues(t, 1, attempts.Load())
}
