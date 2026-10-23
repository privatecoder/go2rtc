package bc

import (
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/bc"
)

func Init() {
	log := app.GetLogger("bc")
	bc.Log = bc.Logger{
		Debugf: func(format string, args ...any) { log.Debug().Msgf(format, args...) },
		Infof:  func(format string, args ...any) { log.Info().Msgf(format, args...) },
		Warnf:  func(format string, args ...any) { log.Warn().Msgf(format, args...) },
	}

	streams.HandleFunc("bc", bc.Dial)
}
