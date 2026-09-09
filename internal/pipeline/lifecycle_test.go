package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"typetype-downloader-go/internal/config"
	"typetype-downloader-go/internal/job"
)

type terminalSink struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (s terminalSink) SaveJob(record *job.Record) {
	if record.Status == job.StatusFailed {
		close(s.entered)
		<-s.release
		close(s.done)
	}
}

func TestWaitIncludesFinalPersistenceNotification(t *testing.T) {
	requested := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requested)
		<-r.Context().Done()
	}))
	defer upstream.Close()
	sink := terminalSink{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	store := job.NewStore("", sink)
	store.Restore([]*job.Record{{ID: "active", URL: "https://www.bilibili.com/video/BV1xx411c7mD", Status: job.StatusQueued}})
	runner := NewRunner(config.Config{MaxWorkers: 1, MaxQueueSize: 2, TypeTypeAPIBase: upstream.URL}, store, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner.Start(ctx)
	if err := runner.Enqueue("active"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requested:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case <-sink.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not persist cancellation")
	}
	waited := make(chan struct{})
	go func() { runner.Wait(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("Wait returned before final notification")
	case <-time.After(20 * time.Millisecond):
	}
	close(sink.release)
	select {
	case <-waited:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	select {
	case <-sink.done:
	default:
		t.Fatal("final notification missing")
	}
}

func TestCancelledRunnerLeavesQueuedJobsForRestart(t *testing.T) {
	store := job.NewStore("")
	store.Restore([]*job.Record{{ID: "queued", Status: job.StatusQueued}})
	runner := NewRunner(config.Config{MaxWorkers: 2, MaxQueueSize: 2}, store, nil, nil)
	if err := runner.Enqueue("queued"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner.Start(ctx)
	runner.Wait()
	record, _ := store.Get("queued")
	if record.Status != job.StatusQueued {
		t.Fatalf("queued job changed: %s", record.Status)
	}
}
