package db

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"typetype-downloader-go/internal/job"
)

type DragonflySink struct {
	client *redis.Client
	ttl    time.Duration
	writer *jobWriter
}

func OpenDragonfly(addr string, ttlSeconds int) *DragonflySink {
	if ttlSeconds <= 0 {
		ttlSeconds = 600
	}
	sink := &DragonflySink{
		client: redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true}),
		ttl:    time.Duration(ttlSeconds) * time.Second,
	}
	sink.writer = newJobWriter(sink.save, time.Second, persistenceShutdownTimeout)
	return sink
}

func (s *DragonflySink) Close() error {
	err := s.writer.close()
	if err != nil {
		slog.Error("dragonfly persistence shutdown", "error", err)
	}
	return errors.Join(err, s.client.Close())
}

func (s *DragonflySink) Name() string { return "dragonfly" }

func (s *DragonflySink) Health(ctx context.Context) error { return s.client.Ping(ctx).Err() }

func (s *DragonflySink) SaveJob(record *job.Record) {
	s.writer.enqueue(record)
}

func (s *DragonflySink) save(ctx context.Context, record *job.Record) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	pipe := s.client.Pipeline()
	pipe.Set(ctx, "downloader:job:"+record.ID, payload, s.ttl)
	pipe.Set(ctx, "downloader:status:"+record.ID, string(record.Status), s.ttl)
	if record.CacheKey != "" && record.Status == job.StatusDone {
		pipe.Set(ctx, "downloader:cache:"+record.CacheKey, record.ID, s.ttl)
	}
	_, err = pipe.Exec(ctx)
	return err
}
