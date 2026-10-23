package bc

import (
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/bc"
)

func Init() {
	log := app.GetLogger("bc")
	bc.Logf = func(format string, args ...any) {
		log.Warn().Msgf(format, args...)
	}

	streams.HandleFunc("bc", bc.Dial)
}
