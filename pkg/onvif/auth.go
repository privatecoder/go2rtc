package onvif

import (
	"bytes"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/xml"
	"io"
	"strings"
	"time"
)

// maxClockSkew is how far the Created time of a UsernameToken may be from now.
const maxClockSkew = 5 * time.Minute

// usernameToken is the content of the WS-Security UsernameToken of a request.
type usernameToken struct {
	username, password, passwordType, nonce, created string
	has                                              map[string]bool
}

// parseUsernameToken reads the one UsernameToken in Envelope > Header >
// Security, by local names, whatever the namespace prefixes are. The document
// must be a single Envelope (only whitespace, comments and processing
// instructions around it, no DOCTYPE) with at most one Header, before the
// one Body. It returns false for anything else: malformed XML, no token, more
// than one, a repeated field, or a field with child elements.
func parseUsernameToken(b []byte) (*usernameToken, bool) {
	b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")) // one UTF-8 BOM is not text
	d := xml.NewDecoder(bytes.NewReader(b))

	var path []string // local names of the open elements
	var tokens []*usernameToken
	var field string // the child of the UsernameToken that is open
	var text strings.Builder
	var rootSeen, headerSeen, bodySeen bool

	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false
		}

		switch t := t.(type) {
		case xml.Directive:
			return nil, false

		case xml.StartElement:
			name := t.Name.Local
			switch {
			case len(path) == 0:
				if rootSeen || name != "Envelope" {
					return nil, false
				}
				rootSeen = true

			case len(path) == 1:
				switch name {
				case "Header":
					if headerSeen || bodySeen {
						return nil, false
					}
					headerSeen = true
				case "Body":
					if bodySeen {
						return nil, false
					}
					bodySeen = true
				}

			case field != "":
				return nil, false // a field holds text only

			case len(path) == 3 && path[1] == "Header" && path[2] == "Security" && name == "UsernameToken":
				tokens = append(tokens, &usernameToken{has: map[string]bool{}})

			case len(path) == 4 && path[1] == "Header" && path[2] == "Security" && path[3] == "UsernameToken":
				cur := tokens[len(tokens)-1]
				switch name {
				case "Username", "Password", "Nonce", "Created":
					if cur.has[name] {
						return nil, false
					}
					cur.has[name] = true
					field = name
					text.Reset()
					if name == "Password" {
						for _, a := range t.Attr {
							if a.Name.Local == "Type" {
								cur.passwordType = a.Value
							}
						}
					}
				}
			}
			path = append(path, name)

		case xml.CharData:
			if len(path) == 0 && len(bytes.TrimSpace(t)) != 0 {
				return nil, false
			}
			if field != "" && len(path) == 5 {
				text.Write(t)
			}

		case xml.EndElement:
			if field != "" && len(path) == 5 {
				cur := tokens[len(tokens)-1]
				switch field {
				case "Username":
					cur.username = text.String()
				case "Password":
					cur.password = text.String()
				case "Nonce":
					cur.nonce = strings.TrimSpace(text.String())
				case "Created":
					cur.created = text.String()
				}
				field = ""
			}
			path = path[:len(path)-1]
		}
	}

	if !rootSeen || !bodySeen || len(tokens) != 1 {
		return nil, false
	}
	return tokens[0], true
}

// CheckUsernameToken verifies the WS-Security UsernameToken in the SOAP header
// of a request against the user and password. It accepts PasswordDigest,
// base64(SHA1(nonce + created + password)) with the Created time within five
// minutes of now, and PasswordText. A nonce is not remembered, so a captured
// request can be replayed in that window.
func CheckUsernameToken(b []byte, username, password string, now time.Time) bool {
	tok, ok := parseUsernameToken(b)
	if !ok || !tok.has["Username"] || !tok.has["Password"] {
		return false
	}

	if subtle.ConstantTimeCompare([]byte(tok.username), []byte(username)) != 1 {
		return false
	}

	typ := tok.passwordType
	if !strings.HasSuffix(typ, "#PasswordDigest") {
		if typ != "" && !strings.HasSuffix(typ, "#PasswordText") {
			return false
		}
		return subtle.ConstantTimeCompare([]byte(tok.password), []byte(password)) == 1
	}

	nonce, err := base64.StdEncoding.DecodeString(tok.nonce)
	if err != nil || len(nonce) == 0 {
		return false
	}

	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(tok.created))
	if err != nil {
		return false
	}
	if d := now.Sub(t); d > maxClockSkew || d < -maxClockSkew {
		return false
	}

	h := sha1.New()
	h.Write(nonce)
	h.Write([]byte(tok.created))
	h.Write([]byte(password))
	want := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(tok.password)), []byte(want)) == 1
}
