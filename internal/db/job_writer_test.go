package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"typetype-downloader-go/internal/job"
)

func TestJobWriterDrainsInOrder(t *testing.T) {
	var saved []*job.Record
	w := newJobWriter(func(_ context.Context, r *job.Record) error {
		saved = append(saved, r)
		return nil
	}, time.Second, time.Second)
	want := []*job.Record{{}, {}, {}}
	for _, r := range want {
		w.enqueue(r)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	w.enqueue(&job.Record{})
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved, want) || len(saved) != 3 {
		t.Fatalf("saved %d records, want 3 in order", len(saved))
	}
	for i := range want {
		if saved[i] != want[i] {
			t.Fatal("record order changed")
		}
	}
	if len(w.queue) != 0 || w.ctx.Err() == nil {
		t.Fatal("writer retained queued records or an active context")
	}
}

func TestJobWriterReportsFailureAndContinues(t *testing.T) {
	want := errors.New("write failed")
	calls := 0
	w := newJobWriter(func(context.Context, *job.Record) error {
		calls++
		if calls == 1 {
			return want
		}
		return nil
	}, time.Second, time.Second)
	w.enqueue(&job.Record{})
	w.enqueue(&job.Record{})
	if err := w.close(); !errors.Is(err, want) || calls != 2 {
		t.Fatalf("close=%v calls=%d", err, calls)
	}
}

func TestJobWriterWriteDeadline(t *testing.T) {
	finished := make(chan struct{})
	w := newJobWriter(func(ctx context.Context, _ *job.Record) error {
		<-ctx.Done()
		close(finished)
		return ctx.Err()
	}, 10*time.Millisecond, time.Second)
	w.enqueue(&job.Record{})
	select {
	case <-finished:
	case <-time.After(time.Second):
		w.cancel()
		_ = w.close()
		t.Fatal("write deadline was not propagated")
	}
	if err := w.close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close=%v", err)
	}
}

func TestJobWriterShutdownCancelsPendingWrites(t *testing.T) {
	started := make(chan struct{})
	w := newJobWriter(func(ctx context.Context, _ *job.Record) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, time.Hour, 20*time.Millisecond)
	w.enqueue(&job.Record{})
	<-started
	w.enqueue(&job.Record{})
	if err := w.close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close=%v", err)
	}
	if w.failures != 2 || len(w.queue) != 0 {
		t.Fatalf("failures=%d pending=%d", w.failures, len(w.queue))
	}
}

func TestJobWriterReportsFullQueue(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	w := newJobWriter(func(context.Context, *job.Record) error {
		once.Do(func() { close(started); <-release })
		return nil
	}, time.Second, time.Second)
	w.enqueue(&job.Record{})
	<-started
	for i := 0; i <= cap(w.queue); i++ {
		w.enqueue(&job.Record{})
	}
	close(release)
	if err := w.close(); err == nil || !strings.Contains(err.Error(), "rejected 1 writes") {
		t.Fatalf("close=%v", err)
	}
}

func TestJobWriterConcurrentCloseAndEnqueue(t *testing.T) {
	w := newJobWriter(func(context.Context, *job.Record) error { return nil }, time.Second, time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				w.enqueue(&job.Record{})
			}
			if err := w.close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
