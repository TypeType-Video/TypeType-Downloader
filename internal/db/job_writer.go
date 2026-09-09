package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"typetype-downloader-go/internal/job"
)

const persistenceShutdownTimeout = 5 * time.Second

type jobWriter struct {
	queue           chan *job.Record
	done            chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	save            func(context.Context, *job.Record) error
	writeTimeout    time.Duration
	shutdownTimeout time.Duration
	mu              sync.RWMutex
	closed          bool
	closeOnce       sync.Once
	rejected        atomic.Int64
	failures        int
	lastError       error
}

func newJobWriter(save func(context.Context, *job.Record) error, writeTimeout, shutdownTimeout time.Duration) *jobWriter {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &jobWriter{
		queue: make(chan *job.Record, 2048), done: make(chan struct{}),
		ctx: ctx, cancel: cancel, save: save,
		writeTimeout: writeTimeout, shutdownTimeout: shutdownTimeout,
	}
	go writer.run()
	return writer
}

func (w *jobWriter) enqueue(record *job.Record) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return
	}
	select {
	case w.queue <- record:
	default:
		w.rejected.Add(1)
	}
}

func (w *jobWriter) run() {
	defer close(w.done)
	for record := range w.queue {
		if err := w.ctx.Err(); err != nil {
			w.failures++
			w.lastError = err
			continue
		}
		ctx, cancel := context.WithTimeout(w.ctx, w.writeTimeout)
		err := w.save(ctx, record)
		cancel()
		if err != nil {
			w.failures++
			w.lastError = err
		}
	}
}

func (w *jobWriter) close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
		timer := time.AfterFunc(w.shutdownTimeout, w.cancel)
		<-w.done
		timer.Stop()
		w.cancel()
	})
	if w.failures > 0 {
		return fmt.Errorf("persistence: %d writes failed, %d rejected: %w", w.failures, w.rejected.Load(), w.lastError)
	}
	if rejected := w.rejected.Load(); rejected > 0 {
		return fmt.Errorf("persistence queue rejected %d writes", rejected)
	}
	return nil
}
