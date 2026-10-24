package onvif

import (
	"encoding/xml"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Captured from the code before PTZ was added, GetCapabilities/GetServices for
// host 192.168.1.50:1984 and GetProfiles for the names cam and cam-sub.
const goldenCapabilities = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><tds:GetCapabilitiesResponse>\n\t<tds:Capabilities>\n\t\t<tt:Device>\n\t\t\t<tt:XAddr>http://192.168.1.50:1984/onvif/device_service</tt:XAddr>\n\t\t</tt:Device>\n\t\t<tt:Media>\n\t\t\t<tt:XAddr>http://192.168.1.50:1984/onvif/media_service</tt:XAddr>\n\t\t\t<tt:StreamingCapabilities>\n\t\t\t\t<tt:RTPMulticast>false</tt:RTPMulticast>\n\t\t\t\t<tt:RTP_TCP>false</tt:RTP_TCP>\n\t\t\t\t<tt:RTP_RTSP_TCP>true</tt:RTP_RTSP_TCP>\n\t\t\t</tt:StreamingCapabilities>\n\t\t</tt:Media>\n\t</tds:Capabilities>\n</tds:GetCapabilitiesResponse></s:Body></s:Envelope>"
const goldenServices = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><tds:GetServicesResponse>\n\t<tds:Service>\n\t\t<tds:Namespace>http://www.onvif.org/ver10/device/wsdl</tds:Namespace>\n\t\t<tds:XAddr>http://192.168.1.50:1984/onvif/device_service</tds:XAddr>\n\t\t<tds:Version><tt:Major>2</tt:Major><tt:Minor>5</tt:Minor></tds:Version>\n\t</tds:Service>\n\t<tds:Service>\n\t\t<tds:Namespace>http://www.onvif.org/ver10/media/wsdl</tds:Namespace>\n\t\t<tds:XAddr>http://192.168.1.50:1984/onvif/media_service</tds:XAddr>\n\t\t<tds:Version><tt:Major>2</tt:Major><tt:Minor>5</tt:Minor></tds:Version>\n\t</tds:Service>\n</tds:GetServicesResponse></s:Body></s:Envelope>"
const goldenProfiles = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><trt:GetProfilesResponse><trt:Profiles token=\"cam\" fixed=\"true\"><tt:Name>cam</tt:Name><tt:VideoSourceConfiguration token=\"cam\" fixed=\"true\">\n\t<tt:Name>VSC</tt:Name>\n\t<tt:SourceToken>cam</tt:SourceToken>\n\t<tt:Bounds x=\"0\" y=\"0\" width=\"1920\" height=\"1080\"></tt:Bounds>\n</tt:VideoSourceConfiguration><tt:VideoEncoderConfiguration token=\"vec\">\n\t\t<tt:Name>VEC</tt:Name>\n        <tt:UseCount>1</tt:UseCount>\n\t\t<tt:Encoding>H264</tt:Encoding>\n\t\t<tt:Resolution><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:Resolution>\n        <tt:Quality>0</tt:Quality>\n\t\t<tt:RateControl><tt:FrameRateLimit>30</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>8192</tt:BitrateLimit></tt:RateControl>\n        <tt:H264><tt:GovLength>10</tt:GovLength><tt:H264Profile>Main</tt:H264Profile></tt:H264>\n        <tt:SessionTimeout>PT10S</tt:SessionTimeout>\n\t</tt:VideoEncoderConfiguration></trt:Profiles><trt:Profiles token=\"cam-sub\" fixed=\"true\"><tt:Name>cam-sub</tt:Name><tt:VideoSourceConfiguration token=\"cam-sub\" fixed=\"true\">\n\t<tt:Name>VSC</tt:Name>\n\t<tt:SourceToken>cam-sub</tt:SourceToken>\n\t<tt:Bounds x=\"0\" y=\"0\" width=\"1920\" height=\"1080\"></tt:Bounds>\n</tt:VideoSourceConfiguration><tt:VideoEncoderConfiguration token=\"vec\">\n\t\t<tt:Name>VEC</tt:Name>\n        <tt:UseCount>1</tt:UseCount>\n\t\t<tt:Encoding>H264</tt:Encoding>\n\t\t<tt:Resolution><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:Resolution>\n        <tt:Quality>0</tt:Quality>\n\t\t<tt:RateControl><tt:FrameRateLimit>30</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>8192</tt:BitrateLimit></tt:RateControl>\n        <tt:H264><tt:GovLength>10</tt:GovLength><tt:H264Profile>Main</tt:H264Profile></tt:H264>\n        <tt:SessionTimeout>PT10S</tt:SessionTimeout>\n\t</tt:VideoEncoderConfiguration></trt:Profiles></trt:GetProfilesResponse></s:Body></s:Envelope>"
const goldenProfile = "<?xml version=\"1.0\" encoding=\"utf-8\"?><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:tt=\"http://www.onvif.org/ver10/schema\" xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\" xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\"><s:Body><trt:GetProfileResponse><trt:Profile token=\"cam\" fixed=\"true\"><tt:Name>cam</tt:Name><tt:VideoSourceConfiguration token=\"cam\" fixed=\"true\">\n\t<tt:Name>VSC</tt:Name>\n\t<tt:SourceToken>cam</tt:SourceToken>\n\t<tt:Bounds x=\"0\" y=\"0\" width=\"1920\" height=\"1080\"></tt:Bounds>\n</tt:VideoSourceConfiguration><tt:VideoEncoderConfiguration token=\"vec\">\n\t\t<tt:Name>VEC</tt:Name>\n        <tt:UseCount>1</tt:UseCount>\n\t\t<tt:Encoding>H264</tt:Encoding>\n\t\t<tt:Resolution><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:Resolution>\n        <tt:Quality>0</tt:Quality>\n\t\t<tt:RateControl><tt:FrameRateLimit>30</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>8192</tt:BitrateLimit></tt:RateControl>\n        <tt:H264><tt:GovLength>10</tt:GovLength><tt:H264Profile>Main</tt:H264Profile></tt:H264>\n        <tt:SessionTimeout>PT10S</tt:SessionTimeout>\n\t</tt:VideoEncoderConfiguration></trt:Profile></trt:GetProfileResponse></s:Body></s:Envelope>"

const goldenHost = "192.168.1.50:1984"

func TestCapabilitiesUnchangedWithoutPTZ(t *testing.T) {
	require.Equal(t, goldenCapabilities, string(GetCapabilitiesResponse(goldenHost, false)))
	require.Equal(t, goldenServices, string(GetServicesResponse(goldenHost, false)))
	require.Equal(t, goldenProfiles, string(GetProfilesResponse([]string{"cam", "cam-sub"}, nil)))
	require.Equal(t, goldenProfile, string(GetProfileResponse("cam", nil)))

	// features for other names, or none at all, change nothing
	require.Equal(t, goldenProfiles, string(GetProfilesResponse([]string{"cam", "cam-sub"}, map[string]PTZFeatures{"other": {true, true}})))
	require.Equal(t, goldenProfiles, string(GetProfilesResponse([]string{"cam", "cam-sub"}, map[string]PTZFeatures{"cam": {}})))
}

func TestCapabilitiesWithPTZ(t *testing.T) {
	c := string(GetCapabilitiesResponse(goldenHost, true))
	require.Contains(t, c, "<tt:PTZ>")
	require.Contains(t, c, "<tt:XAddr>http://"+goldenHost+"/onvif/ptz_service</tt:XAddr>")
	require.Less(t, strings.Index(c, "<tt:Media>"), strings.Index(c, "<tt:PTZ>")) // schema order
	require.Contains(t, c, "media_service")
	s := string(GetServicesResponse(goldenHost, true))
	require.Contains(t, s, "<tds:Namespace>http://www.onvif.org/ver20/ptz/wsdl</tds:Namespace>")
	require.Contains(t, s, "<tds:XAddr>http://"+goldenHost+"/onvif/ptz_service</tds:XAddr>")
	wellFormed(t, c, s)
}

func TestProfileWithPTZ(t *testing.T) {
	both := string(GetProfilesResponse([]string{"cam", "plain"}, map[string]PTZFeatures{"cam": {PanTilt: true, Zoom: true}}))
	require.Contains(t, both, `<tt:PTZConfiguration token="ptz_cam">`)
	require.Less(t, strings.Index(both, "VideoEncoderConfiguration"), strings.Index(both, "PTZConfiguration"))
	require.Contains(t, both, "<tt:NodeToken>ptz_node_cam</tt:NodeToken>")
	require.Contains(t, both, "<tt:DefaultContinuousPanTiltVelocitySpace>http://www.onvif.org/ver10/tptz/PanTiltSpaces/VelocityGenericSpace</tt:DefaultContinuousPanTiltVelocitySpace>")
	require.Contains(t, both, "<tt:DefaultContinuousZoomVelocitySpace>http://www.onvif.org/ver10/tptz/ZoomSpaces/VelocityGenericSpace</tt:DefaultContinuousZoomVelocitySpace>")
	require.Contains(t, both, "<tt:DefaultPTZTimeout>PT10S</tt:DefaultPTZTimeout>")
	require.Equal(t, 1, strings.Count(both, "<tt:PTZConfiguration"), "only the stream with features")
	require.NotContains(t, both, "ptz_plain")

	pt := string(GetProfileResponse("cam", &PTZFeatures{PanTilt: true}))
	require.Contains(t, pt, "DefaultContinuousPanTiltVelocitySpace")
	require.NotContains(t, pt, "Zoom")

	zoom := string(GetProfileResponse("cam", &PTZFeatures{Zoom: true}))
	require.Contains(t, zoom, "DefaultContinuousZoomVelocitySpace")
	require.NotContains(t, zoom, "PanTilt")

	// no relative or absolute spaces: Frigate must see no autotracking
	for _, s := range []string{both, pt, zoom} {
		require.NotContains(t, s, "Relative")
		require.NotContains(t, s, "Absolute")
		require.NotContains(t, s, "Translation")
	}
	wellFormed(t, both, pt, zoom)
}

func TestPTZResponses(t *testing.T) {
	f := PTZFeatures{PanTilt: true, Zoom: true}
	all := map[string]string{
		"configurations": string(PTZConfigurationsResponse("cam", f)),
		"configuration":  string(PTZConfigurationResponse("cam", f)),
		"options":        string(PTZConfigurationOptionsResponse(f)),
		"nodes":          string(PTZNodesResponse("cam", f, 64)),
		"node":           string(PTZNodeResponse("cam", f, 64)),
		"capabilities":   string(PTZServiceCapabilitiesResponse()),
		"presets":        string(PTZPresetsResponse([]PTZPreset{{"3", "door & <porch>"}, {"7", ""}})),
		"emptypresets":   string(PTZPresetsResponse(nil)),
		"setpreset":      string(PTZSetPresetResponse("3")),
		"goto":           string(PTZEmptyResponse(PTZGotoPreset)),
		"move":           string(PTZEmptyResponse(PTZContinuousMove)),
		"stop":           string(PTZEmptyResponse(PTZStop)),
		"fault":          string(FaultResponse("NoProfile", "no such <profile>")),
	}
	for name, s := range all {
		wellFormed(t, s)
		require.Contains(t, s, "Envelope", name)
		// no autotracking spaces anywhere
		require.NotContains(t, s, "Relative", name)
		require.NotContains(t, s, "Absolute", name)
		require.NotContains(t, s, "Fov", name)
	}

	require.Contains(t, all["configurations"], `<tptz:GetConfigurationsResponse`)
	require.Contains(t, all["configuration"], `<tptz:GetConfigurationResponse`)
	require.Contains(t, all["configuration"], `token="ptz_cam"`)

	o := all["options"]
	require.Contains(t, o, "<tt:ContinuousPanTiltVelocitySpace>")
	require.Contains(t, o, "<tt:ContinuousZoomVelocitySpace>")
	require.Contains(t, o, "<tt:Min>-1</tt:Min><tt:Max>1</tt:Max>")
	require.Contains(t, o, "<tt:Min>PT1S</tt:Min><tt:Max>PT10S</tt:Max>")

	require.Contains(t, all["nodes"], `token="ptz_node_cam"`)
	require.Contains(t, all["nodes"], "<tt:MaximumNumberOfPresets>64</tt:MaximumNumberOfPresets>")
	require.Contains(t, all["node"], `<tptz:GetNodeResponse`)
	require.Contains(t, all["capabilities"], `MoveStatus="false"`)
	require.Contains(t, all["capabilities"], `StatusPosition="false"`)

	p := all["presets"]
	require.Contains(t, p, `<tptz:Preset token="3"><tt:Name>door &amp; &lt;porch&gt;</tt:Name></tptz:Preset>`)
	require.Contains(t, p, `<tptz:Preset token="7">`)
	require.Contains(t, all["setpreset"], "<tptz:PresetToken>3</tptz:PresetToken>")
	require.Contains(t, all["goto"], "<tptz:GotoPresetResponse")
	require.Contains(t, all["stop"], "<tptz:StopResponse")

	// features that are off are not offered
	zo := string(PTZConfigurationOptionsResponse(PTZFeatures{Zoom: true}))
	require.NotContains(t, zo, "PanTilt")
	po := string(PTZNodesResponse("cam", PTZFeatures{PanTilt: true}, 64))
	require.NotContains(t, po, "Zoom")
}

func TestFaultIsSOAP(t *testing.T) {
	s := string(FaultResponse("NoToken", "unknown <preset>"))
	require.Contains(t, s, "env:Fault")
	require.Contains(t, s, "env:Receiver")
	require.Contains(t, s, "ter:NoToken")
	require.Contains(t, s, "unknown &lt;preset&gt;")
	wellFormed(t, s)

	var v struct {
		Body struct {
			Fault struct {
				Code struct {
					Value   string `xml:"Value"`
					Subcode struct {
						Value string `xml:"Value"`
					} `xml:"Subcode"`
				} `xml:"Code"`
				Reason struct {
					Text string `xml:"Text"`
				} `xml:"Reason"`
			} `xml:"Fault"`
		} `xml:"Body"`
	}
	require.NoError(t, xml.Unmarshal([]byte(s), &v))
	require.Equal(t, "env:Receiver", v.Body.Fault.Code.Value)
	require.Equal(t, "ter:NoToken", v.Body.Fault.Code.Subcode.Value)
	require.Equal(t, "unknown <preset>", v.Body.Fault.Reason.Text)
}

func wellFormed(t *testing.T, docs ...string) {
	t.Helper()
	for _, d := range docs {
		var v struct {
			XMLName xml.Name
			Inner   []byte `xml:",innerxml"`
		}
		require.NoError(t, xml.Unmarshal([]byte(d), &v), d)
		require.Equal(t, "Envelope", v.XMLName.Local)
	}
}

// The bodies are what zeep (Frigate, Home Assistant) sends: namespaced, with
// attributes; and an un-prefixed form.
const (
	zeepContinuousMove = `<?xml version='1.0' encoding='utf-8'?>
<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope"><soap-env:Body><ns0:ContinuousMove xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl"><ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="http://www.onvif.org/ver10/schema" x="-0.5" y="0.5"/></ns0:Velocity></ns0:ContinuousMove></soap-env:Body></soap-env:Envelope>`

	zeepZoomOnly = `<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope"><soap-env:Body><ns0:ContinuousMove xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl"><ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:Zoom xmlns:ns1="http://www.onvif.org/ver10/schema" x="1"/></ns0:Velocity></ns0:ContinuousMove></soap-env:Body></soap-env:Envelope>`

	plainContinuousMove = `<Envelope><Body><ContinuousMove xmlns="http://www.onvif.org/ver20/ptz/wsdl"><ProfileToken>cam</ProfileToken><Velocity><PanTilt x="1" y="0" xmlns="http://www.onvif.org/ver10/schema"/><Zoom x="-0.25" xmlns="http://www.onvif.org/ver10/schema"/></Velocity><Timeout>PT0.5S</Timeout></ContinuousMove></Body></Envelope>`
)

func TestParseContinuousMove(t *testing.T) {
	r, err := ParseContinuousMove([]byte(zeepContinuousMove))
	require.NoError(t, err)
	require.Equal(t, "cam", r.Token)
	require.True(t, r.HasPanTilt)
	require.False(t, r.HasZoom)
	require.Equal(t, -0.5, r.PanTilt.X)
	require.Equal(t, 0.5, r.PanTilt.Y)
	require.Equal(t, time.Duration(0), r.Timeout)

	r, err = ParseContinuousMove([]byte(zeepZoomOnly))
	require.NoError(t, err)
	require.False(t, r.HasPanTilt)
	require.True(t, r.HasZoom)
	require.Equal(t, 1.0, r.Zoom)

	r, err = ParseContinuousMove([]byte(plainContinuousMove))
	require.NoError(t, err)
	require.True(t, r.HasPanTilt && r.HasZoom)
	require.Equal(t, 1.0, r.PanTilt.X)
	require.Equal(t, -0.25, r.Zoom)
	require.Equal(t, 500*time.Millisecond, r.Timeout)

	for _, bad := range []string{
		"garbage",
		"",
		`<Envelope><Body><ContinuousMove><ProfileToken>cam</ProfileToken></ContinuousMove></Body></Envelope>`, // no velocity
		`<Envelope><Body><ContinuousMove><Velocity><PanTilt x="a" y="0"/></Velocity></ContinuousMove></Body></Envelope>`,
		`<Envelope><Body><ContinuousMove><Velocity><PanTilt x="1" y="0"/></Velocity><Timeout>soon</Timeout></ContinuousMove></Body></Envelope>`,
	} {
		_, err := ParseContinuousMove([]byte(bad))
		require.Error(t, err, bad)
	}

	// odd numbers are parsed as sent, the controller clamps them
	r, err = ParseContinuousMove([]byte(`<E><B><ContinuousMove><Velocity><PanTilt x="NaN" y="7"/></Velocity></ContinuousMove></B></E>`))
	require.NoError(t, err)
	require.True(t, math.IsNaN(r.PanTilt.X))
	require.Equal(t, 7.0, r.PanTilt.Y)
}

func TestParseTimeout(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"PT0.5S":   500 * time.Millisecond,
		"PT10S":    10 * time.Second,
		"PT1M":     time.Minute,
		"PT1H30M":  90 * time.Minute,
		"PT1M0.5S": time.Minute + 500*time.Millisecond,
		"PT0S":     0,
	} {
		got, err := parseDuration(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, bad := range []string{"", "10", "PT", "P", "PTS", "-PT1S", "PT-1S", "PT1X"} {
		_, err := parseDuration(bad)
		require.Error(t, err, bad)
	}
}

func TestParseStopDefaults(t *testing.T) {
	token, panTilt, zoom := ParseStop([]byte(`<E><B><ns0:Stop xmlns:ns0="x"><ns0:ProfileToken>cam</ns0:ProfileToken></ns0:Stop></B></E>`))
	require.Equal(t, "cam", token)
	require.True(t, panTilt)
	require.True(t, zoom)

	_, panTilt, zoom = ParseStop([]byte(`<E><B><Stop><ProfileToken>cam</ProfileToken><PanTilt>true</PanTilt><Zoom>false</Zoom></Stop></B></E>`))
	require.True(t, panTilt)
	require.False(t, zoom)

	_, panTilt, zoom = ParseStop([]byte(`<E><B><Stop><ProfileToken>cam</ProfileToken><PanTilt>0</PanTilt><Zoom>1</Zoom></Stop></B></E>`))
	require.False(t, panTilt)
	require.True(t, zoom)

	// frigate: PanTilt and Zoom true
	_, panTilt, zoom = ParseStop([]byte(`<E><B><ns0:Stop xmlns:ns0="x"><ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PanTilt>true</ns0:PanTilt><ns0:Zoom>true</ns0:Zoom></ns0:Stop></B></E>`))
	require.True(t, panTilt && zoom)

	// nothing to read: the defaults
	token, panTilt, zoom = ParseStop(nil)
	require.Equal(t, "", token)
	require.True(t, panTilt && zoom)
}

func TestParsePreset(t *testing.T) {
	token, preset, name := ParsePreset([]byte(`<E><B><ns0:SetPreset xmlns:ns0="x"><ns0:ProfileToken>cam</ns0:ProfileToken><ns0:PresetName>door &amp; porch</ns0:PresetName><ns0:PresetToken>3</ns0:PresetToken></ns0:SetPreset></B></E>`))
	require.Equal(t, "cam", token)
	require.Equal(t, "3", preset)
	require.Equal(t, "door & porch", name)

	token, preset, name = ParsePreset([]byte(`<E><B><GotoPreset><ProfileToken>cam</ProfileToken><PresetToken>99</PresetToken></GotoPreset></B></E>`))
	require.Equal(t, "cam", token)
	require.Equal(t, "99", preset)
	require.Equal(t, "", name)

	token, preset, name = ParsePreset([]byte("garbage"))
	require.Equal(t, "", token+preset+name)
}
