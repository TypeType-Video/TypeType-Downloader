package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"typetype-downloader-go/internal/job"
)

func (r *Runner) Start(ctx context.Context) {
	for range r.cfg.MaxWorkers {
		r.workers.Add(1)
		go func() {
			defer r.workers.Done()
			r.worker(ctx)
		}()
	}
}

func (r *Runner) Wait() {
	r.workers.Wait()
}

func (r *Runner) Enqueue(id string) error {
	select {
	case r.queue <- id:
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

func (r *Runner) EnqueueBlocking(ctx context.Context, id string) error {
	select {
	case r.queue <- id:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) worker(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case id := <-r.queue:
			if ctx.Err() != nil {
				return
			}
			r.process(ctx, id)
		}
	}
}

func (r *Runner) process(parent context.Context, id string) {
	record, ok := r.store.Get(id)
	if !ok || record.Status != job.StatusQueued {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.store.Start(id, cancel)
	defer cancel()
	if err := r.run(ctx, id, record); err != nil {
		code := failureCode(ctx, err)
		r.store.Fail(id, code, err)
		slog.Warn("job failed", "id", id, "error", err)
	}
}
