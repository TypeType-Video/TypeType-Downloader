package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"typetype-downloader-go/internal/api"
	"typetype-downloader-go/internal/artifact"
	"typetype-downloader-go/internal/cleanup"
	"typetype-downloader-go/internal/config"
	"typetype-downloader-go/internal/db"
	"typetype-downloader-go/internal/job"
	"typetype-downloader-go/internal/pipeline"
	"typetype-downloader-go/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config.Load()); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	files, err := artifactStore(cfg)
	if err != nil {
		return err
	}
	sinks, health, restored, pending, cleanupFn, err := sinks(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanupFn()
	store := job.NewStore(cfg.PublicBaseURL, sinks...)
	store.Restore(restored)
	pendingIDs := store.RestorePending(pending)
	disk, err := storage.NewMonitor(cfg.DataDir, cfg.MinFreeBytes, cfg.MinFreePercent)
	if err != nil {
		return err
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	runner := pipeline.NewRunner(cfg, store, files, disk)
	runner.Start(workerCtx)
	defer func() {
		cancelWorkers()
		runner.Wait()
	}()
	cleanup.Start(workerCtx, cfg.DataDir, cfg.StorageBackend)
	for _, id := range pendingIDs {
		if err := runner.EnqueueBlocking(ctx, id); err != nil {
			slog.Warn("failed to restore queued job", "id", id, "error", err)
		}
	}

	health = append(health, files)
	handler := api.NewServer(store, runner, files, disk, health...).Routes()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	slog.Info("server listening", "addr", cfg.HTTPAddr)
	return serve(ctx, server)
}

func artifactStore(cfg config.Config) (artifact.Store, error) {
	if cfg.StorageBackend == "s3" {
		return artifact.NewS3Store(artifact.S3Config{
			Endpoint: cfg.S3Endpoint, PublicEndpoint: cfg.S3PublicEndpoint,
			Region: cfg.S3Region, Bucket: cfg.S3Bucket,
			AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey,
			UseSSL: cfg.S3UseSSL, PublicUseSSL: cfg.S3PublicUseSSL,
			PathStyle: cfg.S3PathStyle, URLTTL: time.Duration(cfg.ArtifactTTL) * time.Second,
		})
	}
	return artifact.NewLocalStore(), nil
}

func sinks(ctx context.Context, cfg config.Config) ([]job.Sink, []api.HealthCheck, []*job.Record, []*job.Record, func(), error) {
	var out []job.Sink
	var health []api.HealthCheck
	cleanup := func() {}
	if cfg.RedisAddr != "" {
		dragonfly := db.OpenDragonfly(cfg.RedisAddr, cfg.JobTTLSeconds)
		out = append(out, dragonfly)
		health = append(health, dragonfly)
		previous := cleanup
		cleanup = func() { previous(); _ = dragonfly.Close() }
	}
	if cfg.DatabaseURL == "" {
		return out, health, nil, nil, cleanup, nil
	}
	postgres, err := db.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		cleanup()
		return nil, nil, nil, nil, nil, err
	}
	restored, err := postgres.LoadDone(ctx)
	if err != nil {
		postgres.Close()
		cleanup()
		return nil, nil, nil, nil, nil, err
	}
	pending, err := postgres.LoadRunnable(ctx)
	if err != nil {
		postgres.Close()
		cleanup()
		return nil, nil, nil, nil, nil, err
	}
	out = append(out, postgres)
	health = append(health, postgres)
	previous := cleanup
	cleanup = func() { previous(); postgres.Close() }
	return out, health, restored, pending, cleanup, nil
}
