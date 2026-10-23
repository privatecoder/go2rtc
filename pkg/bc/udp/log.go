package udp

// Logger receives diagnostics at three levels: Debugf for routine steps,
// Infof for the outcome (the chosen path), Warnf for trouble (retries, loss).
// Nil functions discard.
type Logger struct {
	Debugf, Infof, Warnf func(format string, args ...any)
}

func nop(string, ...any) {}

// Prefix puts a name (UID, host) in front of every message.
func (l Logger) Prefix(name string) Logger {
	wrap := func(f func(string, ...any)) func(string, ...any) {
		if f == nil {
			return nop
		}
		return func(format string, args ...any) { f(name+": "+format, args...) }
	}
	return Logger{wrap(l.Debugf), wrap(l.Infof), wrap(l.Warnf)}
}

// filled replaces nil functions with no-ops.
func (l Logger) Filled() Logger {
	if l.Debugf == nil {
		l.Debugf = nop
	}
	if l.Infof == nil {
		l.Infof = nop
	}
	if l.Warnf == nil {
		l.Warnf = nop
	}
	return l
}

func (l Logger) empty() bool { return l.Debugf == nil && l.Infof == nil && l.Warnf == nil }
