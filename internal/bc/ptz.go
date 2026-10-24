package bc

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/bc"
)

// prefetchTimeout bounds how long the start-up prefetch waits for one camera,
// the request itself carries on and fills the cache.
const prefetchTimeout = 60 * time.Second

// Source is the first bc:// source of the stream, "" if it has none.
func Source(name string) string {
	stream := streams.Get(name)
	if stream == nil {
		return ""
	}
	for _, source := range stream.Sources() {
		if strings.HasPrefix(source, "bc://") {
			return source
		}
	}
	return ""
}

// prefetchPTZ asks every bc:// stream's camera what it can move, so that the
// ONVIF answers have it by the time a client asks. It does not stream, the
// session is the PTZ control session.
func prefetchPTZ() {
	log := app.GetLogger("bc")

	var wg sync.WaitGroup
	for _, name := range streams.GetAllNames() {
		source := Source(name)
		if source == "" {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			p, err := bc.PTZFor(source)
			if err != nil {
				log.Debug().Err(err).Msgf("[bc] ptz prefetch stream=%s", name)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), prefetchTimeout)
			defer cancel()
			caps, err := p.Caps(ctx)
			if err != nil {
				log.Debug().Err(err).Msgf("[bc] ptz prefetch stream=%s", name)
				return
			}
			log.Debug().Msgf("[bc] ptz stream=%s pantilt=%t zoom=%t", name, caps.PanTilt, caps.Zoom)
		}()
	}
	wg.Wait()
}
