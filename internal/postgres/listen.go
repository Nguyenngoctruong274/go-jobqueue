package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// PendingChannel is the NOTIFY channel the jobs table raises when a job
// becomes pending; the payload is "<type> <run_at in Unix microseconds>".
const PendingChannel = "jobqueue_pending"

// Listener receives wake-ups for pending jobs over a dedicated connection
// and forwards each as a job.Wake on C. It reconnects on its own after a
// connection loss and then sends a zero Wake once, since anything may
// have happened meanwhile. Workers keep polling regardless: a wake-up
// only shortens the wait, it is not what makes a job run.
type Listener struct {
	l      *pq.Listener
	c      chan job.Wake
	logger *slog.Logger
	done   chan struct{}
}

// Listen opens a listening connection to the database at url and
// subscribes to PendingChannel. It returns once the subscription is
// active or ctx ends.
func Listen(ctx context.Context, url string, logger *slog.Logger) (*Listener, error) {
	if logger == nil {
		logger = slog.Default()
	}
	l := &Listener{c: make(chan job.Wake, 64), logger: logger, done: make(chan struct{})}
	l.l = pq.NewListener(url, 500*time.Millisecond, 30*time.Second, l.event)

	// pq.Listener dials in the background and Listen blocks until the
	// subscription is active, which never happens if the database is
	// unreachable; ctx bounds the wait.
	listened := make(chan error, 1)
	go func() { listened <- l.l.Listen(PendingChannel) }()
	select {
	case err := <-listened:
		if err != nil {
			_ = l.l.Close()
			return nil, fmt.Errorf("listen on %s: %w", PendingChannel, err)
		}
	case <-ctx.Done():
		_ = l.l.Close()
		<-listened
		return nil, fmt.Errorf("listen on %s: %w", PendingChannel, ctx.Err())
	}
	go l.forward()
	return l, nil
}

// C delivers one Wake per job that became pending. It is closed by Close.
// Sends never block: when the receiver is behind, the wake-up is dropped,
// which is harmless because one wake-up is enough to make a worker look
// at the queue, and the poll catches anything else.
func (l *Listener) C() <-chan job.Wake { return l.c }

// Close ends the subscription and the connection.
func (l *Listener) Close() error {
	err := l.l.Close()
	<-l.done
	return err
}

func (l *Listener) forward() {
	defer close(l.done)
	defer close(l.c)
	for n := range l.l.Notify {
		// pq sends nil after a reconnect: notifications may have been lost.
		var w job.Wake
		if n != nil {
			w = parseWake(n.Extra)
		}
		select {
		case l.c <- w:
		default:
		}
	}
}

// parseWake decodes "<type> <unix µs>". Anything unexpected becomes a
// zero Wake, which is the safe reading: look at the queue now.
func parseWake(payload string) job.Wake {
	typ, micros, ok := strings.Cut(payload, " ")
	if !ok {
		return job.Wake{}
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return job.Wake{}
	}
	return job.Wake{Type: typ, RunAt: time.UnixMicro(us)}
}

func (l *Listener) event(ev pq.ListenerEventType, err error) {
	switch ev {
	case pq.ListenerEventConnected:
		l.logger.Info("job listener connected")
	case pq.ListenerEventReconnected:
		l.logger.Warn("job listener reconnected; waking all workers")
	case pq.ListenerEventDisconnected:
		l.logger.Warn("job listener disconnected; reconnecting", slog.Any("error", err))
	case pq.ListenerEventConnectionAttemptFailed:
		if err != nil && !errors.Is(err, context.Canceled) {
			l.logger.Warn("job listener reconnect failed", slog.Any("error", err))
		}
	}
}
