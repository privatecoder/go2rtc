// Package bc is a client for the Reolink Baichuan protocol.
package bc

import (
	"errors"
	"net/url"
	"regexp"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

var reUID = regexp.MustCompile(`^[0-9A-Z]{10,}$`)

// Dial opens a bc:// source:
//
//	bc://user:pass@192.168.1.20[:9000]?channel=0&stream=main|sub|extern&encryption=aes|bc|none
//
// A bare UID as host means Reolink P2P, which is not implemented yet.
func Dial(rawURL string) (core.Producer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	query := u.Query()

	kind, ok := ParseStreamKind(query.Get("stream"))
	if !ok {
		return nil, errors.New("bc: unknown stream " + query.Get("stream") + ", use main, sub or extern")
	}

	enc, ok := ParseMaxEncryption(query.Get("encryption"))
	if !ok {
		return nil, errors.New("bc: unknown encryption " + query.Get("encryption") + ", use aes, bc or none")
	}

	var channel uint8
	if s := query.Get("channel"); s != "" {
		channel = core.ParseByte(s)
	}

	if u.Port() == "" && reUID.MatchString(u.Hostname()) {
		return nil, errors.New("bc: P2P/UID not implemented yet")
	}

	password, _ := u.User.Password()
	cfg := Config{Username: u.User.Username(), Password: password, Channel: channel, Encryption: enc}

	conn, err := DialTCP(u.Host)
	if err != nil {
		return nil, err
	}

	prod, err := NewProducer(conn, cfg, kind, rawURL)
	if err != nil {
		return nil, err
	}
	return prod, nil
}
