package sabr

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func Download(ctx context.Context, client *http.Client, options Options, progress ProgressFunc) error {
	parts := downloadPartCount(options)
	reporter := newReporter(progress)
	var last error
	for attempt := 1; attempt <= downloadAttempts; attempt++ {
		if attempt > 1 {
			parts = 1
		}
		reporter.beginAttempt()
		if err := downloadAttempt(ctx, client, options, parts, reporter); err == nil {
			reporter.finish()
			return nil
		} else {
			last = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < downloadAttempts {
			if err := retryDelay(ctx, attempt); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("SABR download failed after %d attempts: %w", downloadAttempts, last)
}

func downloadAttempt(
	ctx context.Context,
	client *http.Client,
	options Options,
	parts int,
	progress *reporter,
) error {
	defer cleanupDownloadFiles(options, parts)
	if err := downloadParts(ctx, client, options, parts, progress); err != nil {
		return err
	}
	return assembleDownload(options, parts)
}

func buildDownloadURL(options Options, part int, parts int) (string, error) {
	parsed, err := url.Parse(options.ManifestURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("audioItag", strconv.Itoa(options.AudioItag))
	if options.VideoItag > 0 {
		query.Set("videoItag", strconv.Itoa(options.VideoItag))
	}
	parsed.Path = strings.Replace(parsed.Path, "/sabr/manifest/", "/sabr/download/", 1)
	if options.AudioTrackID != "" {
		query.Set("audioTrackId", options.AudioTrackID)
	}
	if options.AudioOnly {
		query.Set("audioOnly", "true")
	}
	query.Set("part", strconv.Itoa(part))
	query.Set("parts", strconv.Itoa(parts))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func requestDownload(
	ctx context.Context,
	client *http.Client,
	rawURL string,
	authorization string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if value := strings.TrimSpace(authorization); value != "" {
		req.Header.Set("Authorization", value)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("GET %s returned %s", req.URL.Path, response.Status)
	}
	if mediaType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]); mediaType != downloadMediaType {
		response.Body.Close()
		return nil, fmt.Errorf("GET %s returned unexpected content type %q", req.URL.Path, mediaType)
	}
	return response, nil
}

const downloadAttempts = 2

func downloadPartCount(options Options) int {
	if options.Parts > 0 {
		if options.Parts > maxDownloadParts {
			return maxDownloadParts
		}
		return options.Parts
	}
	if options.AudioOnly {
		if options.ExpectedBytes >= 16<<20 {
			return 4
		}
		if options.ExpectedBytes >= 4<<20 {
			return 2
		}
		return 1
	}
	switch {
	case options.ExpectedBytes >= 256<<20:
		return 12
	case options.ExpectedBytes >= 128<<20:
		return 6
	case options.ExpectedBytes >= 16<<20:
		return 4
	case options.ExpectedBytes >= 4<<20:
		return 2
	default:
		return 1
	}
}

func retryDelay(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

const maxDownloadParts = 12
