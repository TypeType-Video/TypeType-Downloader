package pipeline

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"typetype-downloader-go/internal/artifact"
	"typetype-downloader-go/internal/config"
	"typetype-downloader-go/internal/job"
	"typetype-downloader-go/internal/selector"
	"typetype-downloader-go/internal/storage"
	"typetype-downloader-go/internal/typetype"
)

type Runner struct {
	cfg     config.Config
	store   *job.Store
	streams *typetype.Client
	storage artifact.Store
	disk    *storage.Monitor
	http    *http.Client
	queue   chan string
	workers sync.WaitGroup
}

func NewRunner(cfg config.Config, store *job.Store, files artifact.Store, disk *storage.Monitor) *Runner {
	return &Runner{
		cfg:     cfg,
		store:   store,
		streams: typetype.NewClient(cfg.TypeTypeAPIBase),
		storage: files,
		disk:    disk,
		http:    newHTTPClient(cfg.DownloadWorkers, cfg.HTTP2),
		queue:   make(chan string, cfg.MaxQueueSize),
	}
}

func (r *Runner) run(ctx context.Context, id string, record *job.Record) error {
	started := time.Now()
	var stream *typetype.StreamResponse
	if err := retry(ctx, 3, "stream extraction", func() error {
		var fetchErr error
		stream, fetchErr = r.streams.FetchStream(ctx, record.URL, record.Authorization)
		return fetchErr
	}); err != nil {
		return err
	}
	if isAudioOnly(record.Options) {
		return r.runAudioOnly(ctx, id, record, stream, started)
	}
	selection, err := selector.SelectMP4WithOptions(stream, selectorOptions(record.Options))
	if err != nil {
		return err
	}
	release, err := r.reserveVideo(id, selection, stream.Duration)
	if err != nil {
		return err
	}
	defer release()
	if selection.Video.DeliveryMethod == "sabr" {
		return r.runSABR(ctx, id, record, stream.Title, selection)
	}
	if usesRemoteMux(selection) {
		return r.runRemoteMux(ctx, id, stream.Title, selection)
	}
	if selection.Video.ContentLength <= 0 || selection.Audio.ContentLength <= 0 {
		return r.runRemote(ctx, id, stream.Title, selection)
	}
	paths := artifact.Build(r.cfg.DataDir, id, stream.Title, selection.Container)
	preserveOutput := false
	defer func() { cleanupWork(paths, preserveOutput) }()
	resolved := resolvedOutput(selection, paths.Name)
	r.store.Resolve(id, stream.Title, resolved)
	if err := os.MkdirAll(paths.WorkDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(paths.Output), 0o755); err != nil {
		return err
	}
	downloadMs, err := r.download(ctx, id, selection, paths)
	if err != nil {
		return err
	}
	muxStarted := time.Now()
	totalBytes := selection.Video.ContentLength + selection.Audio.ContentLength
	if totalBytes <= 0 {
		if current, ok := r.store.Get(id); ok {
			totalBytes = current.Progress.TotalBytes
		}
	}
	r.store.Progress(id, job.Progress{Stage: "mux", DownloadedBytes: totalBytes, TotalBytes: totalBytes})
	if err := retry(ctx, 2, "mux", func() error {
		_ = os.Remove(paths.Output)
		return merge(ctx, r.cfg.Muxer, paths.Video, paths.Audio, paths.Output)
	}); err != nil {
		return err
	}
	muxMs := time.Since(muxStarted).Milliseconds()
	var saved artifact.Saved
	if err := retry(ctx, 3, "artifact upload", func() error {
		var saveErr error
		saved, saveErr = r.storage.Save(ctx, paths.Output, paths.Key)
		return saveErr
	}); err != nil {
		return err
	}
	expiresAt := saved.Expires
	var expires *time.Time
	if !expiresAt.IsZero() {
		expires = &expiresAt
	}
	r.store.Done(id, saved.Location, saved.Backend, expires, downloadMs, muxMs)
	preserveOutput = saved.Backend == "local"
	slog.Info("job completed", "id", id, "ms", time.Since(started).Milliseconds())
	return nil
}

func (r *Runner) runAudioOnly(ctx context.Context, id string, record *job.Record, stream *typetype.StreamResponse, started time.Time) error {
	selection, err := selector.SelectAudioOnly(stream, selectorOptions(record.Options))
	if err != nil {
		return err
	}
	release, err := r.reserveAudio(id, selection, stream.Duration)
	if err != nil {
		return err
	}
	defer release()
	if selection.Audio.DeliveryMethod == "sabr" {
		return r.runSABRAudio(ctx, id, record, stream.Title, selection)
	}
	if selection.Audio.ContentLength <= 0 {
		return r.runRemoteAudio(ctx, id, stream.Title, selection)
	}
	paths := artifact.Build(r.cfg.DataDir, id, stream.Title, selection.Container)
	preserveOutput := false
	defer func() { cleanupWork(paths, preserveOutput) }()
	resolved := audioResolvedOutput(selection, paths.Name)
	r.store.Resolve(id, stream.Title, resolved)
	if err := os.MkdirAll(paths.WorkDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(paths.Output), 0o755); err != nil {
		return err
	}
	downloadMs, err := r.downloadAudio(ctx, id, selection, paths)
	if err != nil {
		return err
	}
	var saved artifact.Saved
	if err := retry(ctx, 3, "artifact upload", func() error {
		var saveErr error
		saved, saveErr = r.storage.Save(ctx, paths.Output, paths.Key)
		return saveErr
	}); err != nil {
		return err
	}
	expiresAt := saved.Expires
	var expires *time.Time
	if !expiresAt.IsZero() {
		expires = &expiresAt
	}
	r.store.Done(id, saved.Location, saved.Backend, expires, downloadMs, 0)
	preserveOutput = saved.Backend == "local"
	slog.Info("audio job completed", "id", id, "ms", time.Since(started).Milliseconds())
	return nil
}
