package bc

import (
	"strings"

	"github.com/AlexxIT/go2rtc/internal/streams"
)

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
