package onvif

import (
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/stretchr/testify/require"
)

const (
	authUser = "admin"
	authPass = "s3cret"
	remote   = "192.168.1.20:41000"
)

func withAuth(t *testing.T, user, pass string, localAuth bool) {
	t.Helper()
	setVar(t, &apiAuth, func() (string, string, bool) { return user, pass, localAuth })
}

func digestHeader(user, pass string) string {
	nonce := []byte("0123456789abcdef")
	created := time.Now().UTC().Format(time.RFC3339Nano)
	h := sha1.New()
	h.Write(nonce)
	h.Write([]byte(created))
	h.Write([]byte(pass))
	return `<s:Header><wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><wsse:UsernameToken><wsse:Username>` + user +
		`</wsse:Username><wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">` + base64.StdEncoding.EncodeToString(h.Sum(nil)) +
		`</wsse:Password><wsse:Nonce>` + base64.StdEncoding.EncodeToString(nonce) + `</wsse:Nonce><wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">` +
		created + `</wsu:Created></wsse:UsernameToken></wsse:Security></s:Header>`
}

// authBody is a request for op with an optional SOAP header.
func authBody(header, op string) string {
	return `<?xml version="1.0"?><s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">` + header +
		`<s:Body><` + op + ` xmlns="http://www.onvif.org/ver10/media/wsdl"/></s:Body></s:Envelope>`
}

func callFrom(t *testing.T, remoteAddr, path, body string, basic bool) (int, string, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "http://"+testHost+path, strings.NewReader(body))
	req.RemoteAddr = remoteAddr
	if basic {
		req.SetBasicAuth(authUser, authPass)
	}
	w := httptest.NewRecorder()
	onvifDeviceService(w, req)
	res := w.Result()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header.Get("Content-Type"), string(b)
}

func requireNotAuthorized(t *testing.T, code int, ct, b string) {
	t.Helper()
	require.Equal(t, 400, code, b)
	require.Contains(t, ct, "soap+xml")
	require.Equal(t, "ter:NotAuthorized", faultCode(t, b))
	require.Contains(t, b, "env:Sender")
	require.Contains(t, b, "Sender not authorized")
}

func TestAuthRequiredForProtectedOperations(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	for _, op := range []string{"GetProfiles", "GetDeviceInformation", "GetStreamUri", "GetScopes", "SystemReboot", "Unknown"} {
		code, ct, b := callFrom(t, remote, "/onvif/device_service", authBody("", op), false)
		requireNotAuthorized(t, code, ct, b)
	}
	code, ct, b := callFrom(t, remote, "/onvif/media_service", authBody("", "GetProfiles"), false)
	requireNotAuthorized(t, code, ct, b)
}

func TestAuthWSSecurityDigest(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	code, _, b := callFrom(t, remote, "/onvif/media_service", authBody(digestHeader(authUser, authPass), "GetProfiles"), false)
	require.Equal(t, 200, code, b)
	require.Contains(t, b, "GetProfilesResponse")

	code, ct, b := callFrom(t, remote, "/onvif/media_service", authBody(digestHeader(authUser, "wrong"), "GetProfiles"), false)
	requireNotAuthorized(t, code, ct, b)
	code, ct, b = callFrom(t, remote, "/onvif/media_service", authBody(digestHeader("root", authPass), "GetProfiles"), false)
	requireNotAuthorized(t, code, ct, b)
}

func TestAuthHTTPBasic(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	code, _, b := callFrom(t, remote, "/onvif/media_service", authBody("", "GetProfiles"), true)
	require.Equal(t, 200, code, b)

	req := httptest.NewRequest("POST", "http://"+testHost+"/onvif/media_service", strings.NewReader(authBody("", "GetProfiles")))
	req.RemoteAddr = remote
	req.SetBasicAuth(authUser, "wrong")
	w := httptest.NewRecorder()
	onvifDeviceService(w, req)
	require.Equal(t, 400, w.Code)
}

func TestAuthPreAuthOperations(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	for _, op := range []string{"GetSystemDateAndTime", "GetCapabilities", "GetServices", "GetServiceCapabilities", "GetHostname"} {
		code, _, b := callFrom(t, remote, "/onvif/device_service", authBody("", op), false)
		require.Equal(t, 200, code, op+": "+b)
	}
}

func TestAuthPreAuthDoesNotWarmCameras(t *testing.T) {
	withAuth(t, authUser, authPass, false)
	cam := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})

	var lookups atomic.Int32
	inner := ptzFor
	setVar(t, &ptzFor, func(rawURL string) (ptzController, error) {
		lookups.Add(1)
		return inner(rawURL)
	})

	for _, op := range []string{"GetCapabilities", "GetServices"} {
		code, _, b := callFrom(t, remote, "/onvif/device_service", authBody("", op), false)
		require.Equal(t, 200, code, b)
	}
	warmWG.Wait()
	require.Zero(t, lookups.Load(), "an unauthenticated client must not cause camera logins")

	// with credentials the same request warms them
	code, _, b := callFrom(t, remote, "/onvif/device_service", authBody(digestHeader(authUser, authPass), "GetCapabilities"), false)
	require.Equal(t, 200, code, b)
	warmWG.Wait()
	require.NotZero(t, lookups.Load())
}

func TestAuthPTZ(t *testing.T) {
	withAuth(t, authUser, authPass, false)
	cam := zoomCam()
	setup(t, map[string]*stubPTZ{camURL: cam}, map[string][]string{"cam": {camURL}})

	move := `<ns0:ContinuousMove xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl"><ns0:ProfileToken>cam</ns0:ProfileToken><ns0:Velocity><ns1:PanTilt xmlns:ns1="x" x="1" y="0"/></ns0:Velocity></ns0:ContinuousMove>`

	code, ct, b := callFrom(t, remote, "/onvif/ptz_service", authBody("", ""), false)
	requireNotAuthorized(t, code, ct, b)

	body := `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body>` + move + `</s:Body></s:Envelope>`
	code, ct, b = callFrom(t, remote, "/onvif/ptz_service", body, false)
	requireNotAuthorized(t, code, ct, b)
	require.Empty(t, cam.log(), "no controller call without credentials")

	// even the operations that need no credentials on the other services
	body = `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><ns0:GetServiceCapabilities xmlns:ns0="http://www.onvif.org/ver20/ptz/wsdl"/></s:Body></s:Envelope>`
	code, ct, b = callFrom(t, remote, "/onvif/ptz_service", body, false)
	requireNotAuthorized(t, code, ct, b)

	body = `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">` + digestHeader(authUser, authPass) + `<s:Body>` + move + `</s:Body></s:Envelope>`
	code, _, b = callFrom(t, remote, "/onvif/ptz_service", body, false)
	require.Equal(t, 200, code, b)
	require.Equal(t, []string{"move:1,0,pt=true,zoom=0,z=false,timeout=0s"}, cam.log())
}

func TestAuthLoopback(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	for _, addr := range []string{"127.0.0.1:5000", "[::1]:5000", "@"} {
		code, _, b := callFrom(t, addr, "/onvif/media_service", authBody("", "GetProfiles"), false)
		require.Equal(t, 200, code, addr+b)
	}

	// local_auth makes loopback need the credentials too
	withAuth(t, authUser, authPass, true)
	code, ct, b := callFrom(t, "127.0.0.1:5000", "/onvif/media_service", authBody("", "GetProfiles"), false)
	requireNotAuthorized(t, code, ct, b)
	code, _, b = callFrom(t, "127.0.0.1:5000", "/onvif/media_service", authBody(digestHeader(authUser, authPass), "GetProfiles"), false)
	require.Equal(t, 200, code, b)
}

func TestAuthNotConfigured(t *testing.T) {
	withAuth(t, "", "", false)

	code, _, b := callFrom(t, remote, "/onvif/media_service", authBody("", "GetProfiles"), false)
	require.Equal(t, 200, code, b)

	// a token that is there is not checked either
	code, _, b = callFrom(t, remote, "/onvif/media_service", authBody(digestHeader("x", "y"), "GetProfiles"), false)
	require.Equal(t, 200, code, b)
}

// logTo logs everything of the package to the returned buffer.
func logTo(t *testing.T) *strings.Builder {
	t.Helper()
	var sb strings.Builder
	old := log
	log = zerolog.New(&sb).Level(zerolog.TraceLevel)
	t.Cleanup(func() { log = old })
	return &sb
}

func textHeader(user, pass string) string {
	return `<s:Header><wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><wsse:UsernameToken><wsse:Username>` + user +
		`</wsse:Username><wsse:Password>` + pass + `</wsse:Password></wsse:UsernameToken></wsse:Security></s:Header>`
}

func TestAuthCredentialsAreNotLogged(t *testing.T) {
	withAuth(t, authUser, authPass, false)
	out := logTo(t)

	digest := digestHeader(authUser, authPass)
	wrongDigest := digestHeader(authUser, "wrong-digest-pw")

	for _, tc := range []struct{ header, op string }{
		{textHeader(authUser, authPass), "GetProfiles"},
		{textHeader(authUser, "wrong-text-pw"), "GetProfiles"},
		{digest, "GetProfiles"},
		{wrongDigest, "GetProfiles"},
		{textHeader(authUser, authPass), "NoSuchOperation"}, // the unsupported request dump
		{digest, "NoSuchOperation"},
		{wrongDigest, "NoSuchOperation"},
		// a token where it does not belong, in the body, in a comment, in a broken header
		{`<!-- ` + textHeader(authUser, "comment-pw") + ` -->`, "GetProfiles"},
		{`<s:Header>` + textHeader(authUser, "broken-pw"), "GetProfiles"},
	} {
		callFrom(t, remote, "/onvif/media_service", authBody(tc.header, tc.op), false)
	}
	callFrom(t, remote, "/onvif/media_service", authBody("", `GetProfiles><wsse:UsernameToken xmlns:wsse="x"><wsse:Password>body-pw</wsse:Password></wsse:UsernameToken></x:y`), false)

	logged := out.String()
	require.Contains(t, logged, "server request", "the trace dump is on")
	require.Contains(t, logged, "unsupported request", "the debug dump is on")
	require.Contains(t, logged, "not authorized")
	for _, secret := range []string{authPass, "wrong-text-pw", "wrong-digest-pw", "comment-pw", "broken-pw", "body-pw", "MDEyMzQ1Njc4OWFiY2RlZg", "<wsse:Nonce>", "wsu:Created"} {
		require.NotContains(t, logged, secret)
	}
	// the digests themselves
	for _, h := range []string{digest, wrongDigest} {
		i := strings.Index(h, "PasswordDigest\">") + len("PasswordDigest\">")
		require.NotContains(t, logged, h[i:i+28])
	}
}

func TestRedact(t *testing.T) {
	b := []byte(authBody(textHeader("u", "p"), "GetProfiles"))
	out := string(redact(b))
	require.Contains(t, out, "<Header redacted/>")
	require.Contains(t, out, "<GetProfiles")
	require.NotContains(t, out, "Password")

	// no header: the request as it is
	b = []byte(authBody("", "GetProfiles"))
	require.Equal(t, string(b), string(redact(b)))
}

func TestAuthTokenOfAnotherEnvelope(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	first := authBody("", "GetProfiles")
	second := authBody(textHeader(authUser, authPass), "GetHostname")
	for _, body := range []string{first + second, authBody(textHeader(authUser, authPass), "GetProfiles") + first} {
		code, ct, b := callFrom(t, remote, "/onvif/media_service", body, false)
		requireNotAuthorized(t, code, ct, b)
	}
}

func TestAuthBodyOnlyInComment(t *testing.T) {
	withAuth(t, authUser, authPass, false)

	// a valid header, and the only Body is commented out: GetRequestAction would still find the operation
	body := `<?xml version="1.0"?><s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">` + textHeader(authUser, authPass) +
		`<!-- <s:Body><GetProfiles xmlns="http://www.onvif.org/ver10/media/wsdl"/></s:Body> --></s:Envelope>`
	code, ct, b := callFrom(t, remote, "/onvif/media_service", body, false)
	requireNotAuthorized(t, code, ct, b)
}

// zeepRequest is what zeep (Frigate, Home Assistant) sends: the soap-env prefix.
func zeepRequest(pass, op string) string {
	return `<?xml version='1.0' encoding='utf-8'?>
<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope"><soap-env:Header><wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><wsse:UsernameToken><wsse:Username>admin</wsse:Username><wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">` + pass +
		`</wsse:Password><wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">D1ZmvXF5cAOeIdvixAMuVA==</wsse:Nonce><wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">2026-10-25T12:28:45Z</wsu:Created></wsse:UsernameToken></wsse:Security></soap-env:Header><soap-env:Body><ns0:` + op +
		` xmlns:ns0="http://www.onvif.org/ver10/media/wsdl"/></soap-env:Body></soap-env:Envelope>`
}

func TestRedactZeep(t *testing.T) {
	out := string(redact([]byte(zeepRequest("HwigXrb7REKQMOGHfNItHoHOpHY=", "GetProfiles"))))
	require.Contains(t, out, "GetProfiles", "the operation stays visible")
	require.Contains(t, out, "<Header redacted/>")
	for _, secret := range []string{"HwigXrb7REKQMOGHfNItHoHOpHY=", "D1ZmvXF5cAOeIdvixAMuVA==", "Password", "Nonce", "UsernameToken", "admin"} {
		require.NotContains(t, out, secret)
	}

	// a token outside of the header is cut out, the rest stays
	out = string(redact([]byte(`<a:Envelope xmlns:a="x"><a:Body><o.k-1:UsernameToken xmlns:o.k-1="y"><o.k-1:Password>pw</o.k-1:Password></o.k-1:UsernameToken><GetProfiles/></a:Body></a:Envelope>`)))
	require.Contains(t, out, "<redacted/>")
	require.Contains(t, out, "GetProfiles")
	require.NotContains(t, out, "pw")

	// a secret left over removes the body
	require.Equal(t, "<body redacted/>", string(redact([]byte(`<a:Envelope><!-- <Password>pw</Password> --></a:Envelope>`))))
}

func TestZeepRequestIsLoggedWithoutSecrets(t *testing.T) {
	withAuth(t, authUser, authPass, false)
	out := logTo(t)

	callFrom(t, remote, "/onvif/media_service", zeepRequest("HwigXrb7REKQMOGHfNItHoHOpHY=", "GetProfiles"), false)
	callFrom(t, remote, "/onvif/media_service", zeepRequest("HwigXrb7REKQMOGHfNItHoHOpHY=", "NoSuchOperation"), false)

	logged := out.String()
	require.Contains(t, logged, "GetProfiles")
	require.NotContains(t, logged, "body redacted")
	for _, secret := range []string{"HwigXrb7REKQMOGHfNItHoHOpHY=", "D1ZmvXF5cAOeIdvixAMuVA=="} {
		require.NotContains(t, logged, secret)
	}
}
