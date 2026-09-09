package db

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"typetype-downloader-go/internal/job"
)

func TestPostgresJobRoundTrip(t *testing.T) {
	url := os.Getenv("TYPETYPE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set TYPETYPE_TEST_POSTGRES_URL to an isolated test database")
	}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			config, err := pgxpool.ParseConfig(url)
			if err != nil {
				t.Fatal(err)
			}
			admin, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			name := fmt.Sprintf("downloader_test_%d", time.Now().UnixNano())
			schemaName := pgx.Identifier{name}.Sanitize()
			if _, err := admin.Exec(ctx, "create schema "+schemaName); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if _, err := admin.Exec(cleanup, "drop schema "+schemaName+" cascade"); err != nil {
					t.Error(err)
				}
			}()
			config = config.Copy()
			config.ConnConfig.RuntimeParams["search_path"] = name
			config.ConnConfig.DefaultQueryExecMode = mode
			pool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			if err := migrate(ctx, pool); err != nil {
				t.Fatal(err)
			}
			sink := &PostgresSink{pool: pool}
			now := time.Now().UTC().Truncate(time.Microsecond)
			expires := now.Add(time.Hour)
			record := &job.Record{
				ID: "round-trip", CacheKey: "cache", URL: "https://example.com/video",
				Status: job.StatusQueued, Title: "Sample", QueuedAt: now,
				Options:  job.Options{Mode: "video", Quality: "720p"},
				Progress: job.Progress{Stage: "queued"},
			}
			if err := sink.upsert(ctx, record); err != nil {
				t.Fatal(err)
			}
			queued, err := sink.LoadRunnable(ctx)
			if err != nil || len(queued) != 1 {
				t.Fatalf("queued jobs: %v, %v", queued, err)
			}
			if queued[0].Status != job.StatusQueued || !queued[0].QueuedAt.Equal(now) ||
				!reflect.DeepEqual(queued[0].Options, record.Options) || queued[0].FinishedAt != nil {
				t.Fatalf("unexpected queued record: %+v", queued[0])
			}
			record.Status = job.StatusDone
			record.Artifact = "sample.mp4"
			record.Storage = "s3"
			record.FinishedAt = &now
			record.ExpiresAt = &expires
			record.Resolved = &job.ResolvedOutput{Container: "mp4", Height: 720}
			record.Progress = job.Progress{Stage: "done", DownloadedBytes: 12345, TotalBytes: 12345}
			if err := sink.upsert(ctx, record); err != nil {
				t.Fatal(err)
			}
			done, err := sink.LoadDone(ctx)
			if err != nil || len(done) != 1 {
				t.Fatalf("done jobs: %v, %v", done, err)
			}
			got := done[0]
			if got.Status != job.StatusDone || got.Artifact != record.Artifact || got.Storage != "s3" ||
				got.FinishedAt == nil || !got.FinishedAt.Equal(now) ||
				got.ExpiresAt == nil || !got.ExpiresAt.Equal(expires) ||
				!reflect.DeepEqual(got.Resolved, record.Resolved) || got.Progress != record.Progress {
				t.Fatalf("unexpected completed record: %+v", got)
			}
			queued, err = sink.LoadRunnable(ctx)
			if err != nil || len(queued) != 0 {
				t.Fatalf("completed job remains runnable: %v, %v", queued, err)
			}
		})
	}
}
