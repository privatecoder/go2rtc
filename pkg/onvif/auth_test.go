package onvif

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	wssePasswordDigest = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest"
	wssePasswordText   = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
)

var authNow = time.Date(2025, 3, 4, 12, 0, 0, 0, time.UTC)

func digestOf(nonce []byte, created, password string) string {
	h := sha1.New()
	h.Write(nonce)
	h.Write([]byte(created))
	h.Write([]byte(password))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// tokenBody is a request with a UsernameToken, with p as the namespace prefix of the WSSE elements.
func tokenBody(p, user, passAttr, pass, nonce, created string) []byte {
	if p != "" {
		p += ":"
	}
	s := `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Header><` + p + `Security xmlns` + strings.TrimSuffix(strings.Replace(":"+p, "::", ":", 1), ":") +
		`="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"><` + p + `UsernameToken><` + p + `Username>` + user + `</` + p + `Username>` +
		`<` + p + `Password` + passAttr + `>` + pass + `</` + p + `Password>`
	if nonce != "" {
		s += `<` + p + `Nonce>` + nonce + `</` + p + `Nonce>`
	}
	if created != "" {
		s += `<wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">` + created + `</wsu:Created>`
	}
	s += `</` + p + `UsernameToken></` + p + `Security></s:Header><s:Body><tds:GetProfiles xmlns:tds="x"/></s:Body></s:Envelope>`
	return []byte(s)
}

func digestBody(p, user, password string, nonce []byte, created string) []byte {
	return tokenBody(p, user, ` Type="`+wssePasswordDigest+`"`, digestOf(nonce, created, password),
		base64.StdEncoding.EncodeToString(nonce), created)
}

func TestUsernameTokenDigest(t *testing.T) {
	nonce := []byte("0123456789abcdef")
	created := authNow.Format(time.RFC3339)

	for _, p := range []string{"wsse", "o", ""} {
		b := digestBody(p, "admin", "s3cret", nonce, created)
		require.True(t, CheckUsernameToken(b, "admin", "s3cret", authNow), "prefix %q", p)
		require.False(t, CheckUsernameToken(b, "admin", "other", authNow), "prefix %q", p)
		require.False(t, CheckUsernameToken(b, "root", "s3cret", authNow), "prefix %q", p)
	}
}

func TestUsernameTokenDigestVector(t *testing.T) {
	// base64(SHA1(nonce bytes + Created + password)), the nonce bytes decoded from base64
	nonce, _ := base64.StdEncoding.DecodeString("LKqI6G/AikKCQrN0zqZFlg==")
	created := "2025-03-04T12:00:00Z"
	b := tokenBody("wsse", "admin", ` Type="`+wssePasswordDigest+`"`, digestOf(nonce, created, "pw"), "LKqI6G/AikKCQrN0zqZFlg==", created)
	require.True(t, CheckUsernameToken(b, "admin", "pw", authNow))

	// digest over the base64 text of the nonce, not its bytes, is wrong
	h := sha1.New()
	h.Write([]byte("LKqI6G/AikKCQrN0zqZFlg==" + created + "pw"))
	b = tokenBody("wsse", "admin", ` Type="`+wssePasswordDigest+`"`, base64.StdEncoding.EncodeToString(h.Sum(nil)), "LKqI6G/AikKCQrN0zqZFlg==", created)
	require.False(t, CheckUsernameToken(b, "admin", "pw", authNow))
}

func TestUsernameTokenCreated(t *testing.T) {
	nonce := []byte("nonce-bytes-1234")
	check := func(created string) bool {
		return CheckUsernameToken(digestBody("wsse", "u", "p", nonce, created), "u", "p", authNow)
	}

	require.True(t, check(authNow.Format(time.RFC3339)))
	require.True(t, check("2025-03-04T12:00:00.123456Z"), "fractional seconds")
	require.True(t, check("2025-03-04T13:00:00.5+01:00"), "offset instead of Z")
	require.True(t, check(authNow.Add(4*time.Minute+50*time.Second).Format(time.RFC3339)))
	require.True(t, check(authNow.Add(-4*time.Minute-50*time.Second).Format(time.RFC3339)))
	require.False(t, check(authNow.Add(6*time.Minute).Format(time.RFC3339)), "too far ahead")
	require.False(t, check(authNow.Add(-6*time.Minute).Format(time.RFC3339)), "too old")
	require.False(t, check("yesterday"))
	require.False(t, check(""))
}

func TestUsernameTokenDigestNeedsNonceAndCreated(t *testing.T) {
	// the digest of the password alone must not do
	b := tokenBody("wsse", "u", ` Type="`+wssePasswordDigest+`"`, digestOf(nil, "", "p"), "", "")
	require.False(t, CheckUsernameToken(b, "u", "p", authNow))
	b = tokenBody("wsse", "u", ` Type="`+wssePasswordDigest+`"`, digestOf([]byte("n"), "", "p"), base64.StdEncoding.EncodeToString([]byte("n")), "")
	require.False(t, CheckUsernameToken(b, "u", "p", authNow))
	b = tokenBody("wsse", "u", ` Type="`+wssePasswordDigest+`"`, "x", "!!not base64!!", authNow.Format(time.RFC3339))
	require.False(t, CheckUsernameToken(b, "u", "p", authNow))
}

func TestUsernameTokenPasswordText(t *testing.T) {
	for _, attr := range []string{` Type="` + wssePasswordText + `"`, ``, ` Type='` + wssePasswordText + `'`} {
		b := tokenBody("wsse", "admin", attr, "s3cret", "", "")
		require.True(t, CheckUsernameToken(b, "admin", "s3cret", authNow), attr)
		require.False(t, CheckUsernameToken(b, "admin", "s3cre", authNow), attr)
		require.False(t, CheckUsernameToken(b, "other", "s3cret", authNow), attr)
	}

	// attribute order and extra attributes
	b := tokenBody("o", "admin", ` Id="x" Type="`+wssePasswordText+`"`, "s3cret", "", "")
	require.True(t, CheckUsernameToken(b, "admin", "s3cret", authNow))

	// XML escapes in the password
	b = tokenBody("o", "admin", ``, "a&amp;b&lt;c", "", "")
	require.True(t, CheckUsernameToken(b, "admin", "a&b<c", authNow))
}

func TestUsernameTokenDigestAttributeOrder(t *testing.T) {
	nonce := []byte("0123456789abcdef")
	created := authNow.Format(time.RFC3339)
	b := tokenBody("wsse", "u", ` Id="p1"  Type = "`+wssePasswordDigest+`"`, digestOf(nonce, created, "p"), base64.StdEncoding.EncodeToString(nonce), created)
	require.True(t, CheckUsernameToken(b, "u", "p", authNow))
}

func TestUsernameTokenMissing(t *testing.T) {
	require.False(t, CheckUsernameToken(nil, "u", "p", authNow))
	require.False(t, CheckUsernameToken([]byte(`<s:Envelope><s:Body><GetProfiles/></s:Body></s:Envelope>`), "u", "p", authNow))
	// a Username/Password outside of a UsernameToken is nothing
	require.False(t, CheckUsernameToken([]byte(`<s:Envelope><s:Body><Username>u</Username><Password>p</Password></s:Body></s:Envelope>`), "u", "p", authNow))
	// an empty password never matches a configured one
	require.False(t, CheckUsernameToken(tokenBody("wsse", "u", ``, "", "", ""), "u", "p", authNow))
}

func TestSenderFaultResponse(t *testing.T) {
	b := SenderFaultResponse("NotAuthorized", "Sender not authorized")

	var v struct {
		Code string `xml:"Body>Fault>Code>Value"`
		Sub  string `xml:"Body>Fault>Code>Subcode>Value"`
		Text string `xml:"Body>Fault>Reason>Text"`
	}
	require.NoError(t, xml.Unmarshal(b, &v), string(b))
	require.Equal(t, "env:Sender", v.Code)
	require.Equal(t, "ter:NotAuthorized", v.Sub)
	require.Equal(t, "Sender not authorized", v.Text)
	require.Contains(t, string(b), `xmlns:ter="http://www.onvif.org/ver10/error"`)

	// the receiver fault is unchanged
	require.Contains(t, string(FaultResponse("NoProfile", "x")), "<env:Value>env:Receiver</env:Value>")
}

const wsseNS = `xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"`

func textToken(user, pass string) string {
	return `<wsse:Security ` + wsseNS + `><wsse:UsernameToken><wsse:Username>` + user + `</wsse:Username><wsse:Password>` + pass + `</wsse:Password></wsse:UsernameToken></wsse:Security>`
}

func envelope(header, body string) []byte {
	return []byte(`<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Header>` + header + `</s:Header><s:Body>` + body + `</s:Body></s:Envelope>`)
}

func TestUsernameTokenPlacement(t *testing.T) {
	good := textToken("u", "p")
	require.True(t, CheckUsernameToken(envelope(good, `<x/>`), "u", "p", authNow))

	for name, b := range map[string][]byte{
		"in body":          envelope(``, good),
		"in body header":   envelope(``, `<s:Header>`+good+`</s:Header>`),
		"in comment":       envelope(`<!--`+good+`-->`, `<x/>`),
		"in cdata":         envelope(`<![CDATA[`+good+`]]>`, `<x/>`),
		"before envelope":  append([]byte(good), envelope(``, `<x/>`)...),
		"after envelope":   append(envelope(``, `<x/>`), []byte(good)...),
		"no security":      envelope(`<wsse:UsernameToken `+wsseNS+`><wsse:Username>u</wsse:Username><wsse:Password>p</wsse:Password></wsse:UsernameToken>`, `<x/>`),
		"nested in header": envelope(`<a>`+good+`</a>`, `<x/>`),
		"not an envelope":  []byte(`<Other><Header>` + good + `</Header></Other>`),
	} {
		require.False(t, CheckUsernameToken(b, "u", "p", authNow), name)
	}
}

func TestUsernameTokenMalformedAndDuplicates(t *testing.T) {
	good := textToken("u", "p")
	bad := textToken("u", "wrong")

	require.False(t, CheckUsernameToken(envelope(good, `<x>`), "u", "p", authNow), "unclosed")
	require.False(t, CheckUsernameToken(append(envelope(good, `<x/>`), []byte(`<`)...), "u", "p", authNow), "garbage after")
	require.False(t, CheckUsernameToken(envelope(good+bad, `<x/>`), "u", "p", authNow), "good then bad")
	require.False(t, CheckUsernameToken(envelope(bad+good, `<x/>`), "u", "p", authNow), "bad then good")
	require.False(t, CheckUsernameToken(envelope(good+good, `<x/>`), "u", "p", authNow), "twice")

	two := `<wsse:Security ` + wsseNS + `><wsse:UsernameToken><wsse:Username>u</wsse:Username><wsse:Password>wrong</wsse:Password><wsse:Password>p</wsse:Password></wsse:UsernameToken></wsse:Security>`
	require.False(t, CheckUsernameToken(envelope(two, `<x/>`), "u", "p", authNow), "two passwords")
}

func TestUsernameTokenCDATAInsideElement(t *testing.T) {
	h := `<wsse:Security ` + wsseNS + `><wsse:UsernameToken><wsse:Username>u</wsse:Username><wsse:Password><![CDATA[a<b]]></wsse:Password></wsse:UsernameToken></wsse:Security>`
	require.True(t, CheckUsernameToken(envelope(h, `<x/>`), "u", "a<b", authNow))
}

func TestUsernameTokenExactText(t *testing.T) {
	// no trimming of what is compared: the user and password are what the API has
	require.True(t, CheckUsernameToken(envelope(textToken(" admin ", " pw "), `<x/>`), " admin ", " pw ", authNow))
	require.False(t, CheckUsernameToken(envelope(textToken(" admin ", " pw "), `<x/>`), "admin", " pw ", authNow))
	require.False(t, CheckUsernameToken(envelope(textToken("admin", "pw"), `<x/>`), " admin ", "pw", authNow))
	require.False(t, CheckUsernameToken(envelope(textToken("admin", " pw "), `<x/>`), "admin", "pw", authNow))

	nonce := []byte("0123456789abcdef")
	created := authNow.Format(time.RFC3339)
	b := digestBody("wsse", " admin ", " pw ", nonce, created)
	require.True(t, CheckUsernameToken(b, " admin ", " pw ", authNow))
	require.False(t, CheckUsernameToken(b, "admin", " pw ", authNow))
}

func TestUsernameTokenCreatedWhitespace(t *testing.T) {
	// the digest covers the Created text as sent, the time is parsed without the spaces
	created := " 2025-03-04T12:00:00Z \n"
	b := digestBody("wsse", "u", "p", []byte("0123456789abcdef"), created)
	require.True(t, CheckUsernameToken(b, "u", "p", authNow))
	// a digest over the trimmed text is not the digest of what was sent
	h := digestBody("wsse", "u", "p", []byte("0123456789abcdef"), strings.TrimSpace(created))
	h = []byte(strings.Replace(string(h), strings.TrimSpace(created)+"</wsu:Created>", created+"</wsu:Created>", 1))
	require.False(t, CheckUsernameToken(h, "u", "p", authNow))
}

func TestUsernameTokenDocumentStructure(t *testing.T) {
	good := string(envelope(textToken("u", "p"), `<GetProfiles/>`))
	noAuth := `<s:Envelope xmlns:s="x"><s:Body><GetProfiles/></s:Body></s:Envelope>`

	for name, tc := range map[string]struct {
		body string
		want bool
	}{
		"valid":                  {good, true},
		"declaration, comments":  {`<?xml version="1.0" encoding="utf-8"?>` + "\n<!-- a -->\n<?pi x?>" + good + "\n<!-- b -->\n", true},
		"extra root before":      {`<unrelated/>` + good, false},
		"extra root after":       {good + `<unrelated/>`, false},
		"text before":            {`garbage` + good, false},
		"text after":             {good + `garbage`, false},
		"token in 2nd envelope":  {noAuth + good, false},
		"token in 1st envelope":  {good + noAuth, false},
		"two good envelopes":     {good + good, false},
		"root is not Envelope":   {`<Other><Header>` + textToken("u", "p") + `</Header></Other>`, false},
		"header after body":      {`<s:Envelope xmlns:s="x"><s:Body><GetProfiles/></s:Body><s:Header>` + textToken("u", "p") + `</s:Header></s:Envelope>`, false},
		"two headers":            {`<s:Envelope xmlns:s="x"><s:Header/><s:Header>` + textToken("u", "p") + `</s:Header><s:Body/></s:Envelope>`, false},
		"two bodies":             {`<s:Envelope xmlns:s="x"><s:Header>` + textToken("u", "p") + `</s:Header><s:Body/><s:Body/></s:Envelope>`, false},
		"header without body":    {`<s:Envelope xmlns:s="x"><s:Header>` + textToken("u", "p") + `</s:Header></s:Envelope>`, false},
		"nested security":        {`<s:Envelope xmlns:s="x"><s:Header><Security>` + textToken("u", "p") + `</Security></s:Header><s:Body/></s:Envelope>`, false},
		"header in body":         {`<s:Envelope xmlns:s="x"><s:Body><s:Header>` + textToken("u", "p") + `</s:Header></s:Body></s:Envelope>`, false},
		"doctype":                {`<!DOCTYPE Envelope>` + good, false},
		"external entity":        {`<!DOCTYPE Envelope [<!ENTITY secret SYSTEM "file:///nonexistent">]>` + strings.Replace(good, `>p</`, `>&secret;</`, 1), false},
		"internal entity":        {`<!DOCTYPE Envelope [<!ENTITY secret "p">]>` + strings.Replace(good, `>p</`, `>&secret;</`, 1), false},
		"child in password":      {strings.Replace(good, `>p</wsse:Password>`, `>p<Other>wrong</Other></wsse:Password>`, 1), false},
		"child before password":  {strings.Replace(good, `<wsse:Password>p`, `<wsse:Password><Other/>p`, 1), false},
		"child in username":      {strings.Replace(good, `>u</wsse:Username>`, `>u<Other/></wsse:Username>`, 1), false},
		"empty child in created": {strings.Replace(good, `</wsse:Password>`, `</wsse:Password><wsse:Created><x/></wsse:Created>`, 1), false},
	} {
		require.Equal(t, tc.want, CheckUsernameToken([]byte(tc.body), "u", "p", authNow), name)
	}
}

func TestUsernameTokenBOM(t *testing.T) {
	const bom = "\xef\xbb\xbf"
	const decl = `<?xml version="1.0" encoding="utf-8"?>`
	nonce := []byte("0123456789abcdef")
	created := authNow.Format(time.RFC3339)

	for name, body := range map[string][]byte{
		"digest": digestBody("wsse", "u", "p", nonce, created),
		"text":   envelope(textToken("u", "p"), `<x/>`),
	} {
		require.True(t, CheckUsernameToken(append([]byte(bom), body...), "u", "p", authNow), name+" BOM")
		require.True(t, CheckUsernameToken(append([]byte(bom+decl), body...), "u", "p", authNow), name+" BOM and declaration")
		require.True(t, CheckUsernameToken(append([]byte(decl), body...), "u", "p", authNow), name+" declaration")
		require.False(t, CheckUsernameToken(append([]byte(bom+bom), body...), "u", "p", authNow), name+" two BOMs")
		require.False(t, CheckUsernameToken(append([]byte(bom+" x"), body...), "u", "p", authNow), name+" text after BOM")
		require.False(t, CheckUsernameToken(append([]byte(bom), body...), "u", "wrong", authNow), name+" BOM, wrong password")
	}
}
