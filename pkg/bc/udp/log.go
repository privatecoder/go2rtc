package udp

import "sync"

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

// Diagnostics of connections are delivered by a few shared workers, not by one
// goroutine per connection: a sink that blocks for good can hold up only
// these workers (events are dropped when the queue is full), it cannot pile up
// a goroutine and a retained connection with every reconnect. Events carry
// the function and the formatted arguments, never the connection.
const (
	diagWorkers = 2
	diagQueue   = 256
)

type diagEvent struct {
	f      func(string, ...any)
	format string
	args   []any
}

var diag struct {
	once sync.Once
	q    chan diagEvent
}

// emit queues a diagnostic. It never blocks.
func emit(f func(string, ...any), format string, args ...any) {
	diag.once.Do(func() {
		diag.q = make(chan diagEvent, diagQueue)
		for i := 0; i < diagWorkers; i++ {
			go func() {
				for e := range diag.q {
					e.f(e.format, e.args...)
				}
			}()
		}
	})
	select {
	case diag.q <- diagEvent{f, format, args}:
	default:
	}
}
