package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"typetype-downloader-go/internal/job"
)

type PostgresSink struct {
	pool  *pgxpool.Pool
	queue chan *job.Record
}

func OpenPostgres(ctx context.Context, databaseURL string) (*PostgresSink, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	sink := &PostgresSink{pool: pool, queue: make(chan *job.Record, 2048)}
	go sink.run()
	return sink, nil
}

func (s *PostgresSink) Close() { s.pool.Close() }

func (s *PostgresSink) Name() string { return "postgres" }

func (s *PostgresSink) Health(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *PostgresSink) SaveJob(record *job.Record) {
	select {
	case s.queue <- record:
	default:
	}
}

func (s *PostgresSink) run() {
	for record := range s.queue {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.upsert(ctx, record)
		cancel()
	}
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}

func (s *PostgresSink) upsert(ctx context.Context, record *job.Record) error {
	options, err := json.Marshal(record.Options)
	if err != nil {
		return err
	}
	progress, err := json.Marshal(record.Progress)
	if err != nil {
		return err
	}
	resolved, err := json.Marshal(record.Resolved)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, upsertSQL,
		record.ID, record.CacheKey, record.URL, string(record.Status), record.Title,
		string(options), string(progress), string(resolved), record.Artifact, record.Storage,
		record.ExpiresAt, stringPtr(record.Error), stringPtr(record.ErrorCode), record.QueuedAt,
		record.StartedAt, record.FinishedAt, record.DownloadMs, record.MuxMs, record.TotalMs,
	)
	if err != nil {
		return fmt.Errorf("upsert job %s: %w", record.ID, err)
	}
	return nil
}

func stringPtr(value *string) *string { return value }
