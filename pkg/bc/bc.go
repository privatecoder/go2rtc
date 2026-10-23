// Package bc is a client for the Reolink Baichuan protocol.
package bc

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/bc/udp"
	"github.com/AlexxIT/go2rtc/pkg/core"
)

var reUID = regexp.MustCompile(`^[0-9A-Z]{10,}$`)

// discoveryTimeout bounds finding a camera by UID.
const discoveryTimeout = 30 * time.Second

// Dial opens a bc:// source:
//
//	bc://user:pass@192.168.1.20[:9000]?channel=0&stream=main|sub|extern&encryption=aes|bc|none
//	bc://user:pass@95270000ABCDEFGH?stream=sub&discovery=local|remote|map|relay&addr=192.168.1.20
//
// A host that is a bare UID (10 or more capital letters and digits, no port)
// is a camera reached over Reolink's P2P service instead of a TCP address.
func Dial(rawURL string) (core.Producer, error) {
	return dial(rawURL, nil)
}

// dial is Dial with a hook to change the discovery options, for tests.
func dial(rawURL string, tune func(*udp.Options)) (core.Producer, error) {
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

	password, _ := u.User.Password()
	cfg := Config{Username: u.User.Username(), Password: password, Channel: channel, Encryption: enc, Name: u.Hostname()}

	if u.Port() == "" && reUID.MatchString(u.Hostname()) {
		method, ok := udp.ParseMethod(query.Get("discovery"))
		if !ok {
			return nil, errors.New("bc: unknown discovery " + query.Get("discovery") + ", use local, remote, map or relay")
		}

		opts := udp.Options{
			Method: method,
			Addr:   query.Get("addr"),
			Region: query.Get("region"),
			Log:    Log.Prefix(u.Hostname()),
		}
		if tune != nil {
			tune(&opts)
		}

		ctx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
		defer cancel()

		conn, err := udp.Discover(ctx, u.Hostname(), opts)
		if err != nil {
			return nil, err
		}

		prod, err := NewProducer(conn, cfg, kind, rawURL)
		if err != nil {
			return nil, err
		}
		prod.Protocol = "udp"
		return prod, nil
	}

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
