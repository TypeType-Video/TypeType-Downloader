package sabr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

func downloadParts(
	ctx context.Context,
	client *http.Client,
	options Options,
	parts int,
	progress *reporter,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, parts)
	partJobs := make(chan int, parts)
	for part := range parts {
		partJobs <- part
	}
	close(partJobs)
	var workers sync.WaitGroup
	for range min(parts, maxConcurrentPartDownloads) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for part := range partJobs {
				if ctx.Err() != nil {
					return
				}
				if err := downloadPartWithRetry(ctx, client, options, part, parts, progress); err != nil {
					select {
					case failures <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func downloadPartWithRetry(
	ctx context.Context,
	client *http.Client,
	options Options,
	part int,
	parts int,
	progress *reporter,
) error {
	var err error
	for attempt := 1; attempt <= partAttempts; attempt++ {
		counter := &partProgress{parent: progress}
		err = downloadPart(ctx, client, options, part, parts, counter)
		if err == nil {
			return nil
		}
		counter.rollback()
		if ctx.Err() != nil || attempt == partAttempts {
			break
		}
		if waitErr := retryDelay(ctx, attempt); waitErr != nil {
			return waitErr
		}
	}
	return err
}

func downloadPart(
	ctx context.Context,
	client *http.Client,
	options Options,
	part int,
	parts int,
	progress byteCounter,
) error {
	partCtx, watchdog := newIdleWatchdog(ctx, options.IdleTimeout)
	defer watchdog.stop()
	rawURL, err := buildDownloadURL(options, part, parts)
	if err != nil {
		return err
	}
	response, err := requestDownload(partCtx, client, rawURL, options.Authorization)
	if err != nil {
		if cause := context.Cause(partCtx); cause != nil {
			err = cause
		}
		return fmt.Errorf("download SABR part %d/%d: %w", part+1, parts, err)
	}
	defer response.Body.Close()
	tracks, err := openPartTracks(options, part)
	if err != nil {
		return err
	}
	defer closeTracks(tracks)
	reader := activityReader{reader: response.Body, touch: watchdog.touch}
	if err := consumeDownloadStream(reader, tracks, progress, part == 0); err != nil {
		if cause := context.Cause(partCtx); cause != nil {
			err = cause
		}
		return fmt.Errorf("consume SABR part %d/%d: %w", part+1, parts, err)
	}
	return closeTracks(tracks)
}

const (
	maxConcurrentPartDownloads = 3
	partAttempts               = 3
)
