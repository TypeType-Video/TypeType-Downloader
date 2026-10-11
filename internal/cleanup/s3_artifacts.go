package cleanup

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"typetype-downloader-go/internal/artifact"
)

const expiredArtifactBatchSize = 100

type ExpiredArtifact struct {
	ID        string
	Location  string
	ExpiresAt time.Time
}

type ExpiredArtifactRepository interface {
	ListExpiredArtifacts(context.Context, time.Time, string, int) ([]ExpiredArtifact, error)
	DeleteExpiredArtifact(context.Context, ExpiredArtifact, time.Time) error
}

type artifactDeleter interface {
	Delete(context.Context, artifact.Saved) error
}

type expiredArtifactJobs interface {
	RemoveExpiredArtifact(string, string, time.Time) bool
}

func StartS3ArtifactCleanup(
	ctx context.Context,
	storageBackend string,
	urlTTL time.Duration,
	files artifactDeleter,
	repository ExpiredArtifactRepository,
	jobs expiredArtifactJobs,
) func() {
	done := make(chan struct{})
	if storageBackend != "s3" {
		close(done)
		return func() { <-done }
	}
	if files == nil || repository == nil {
		slog.Warn("S3 artifact cleanup disabled", "reason", "artifact repository unavailable")
		close(done)
		return func() { <-done }
	}
	go func() {
		defer close(done)
		purgeExpiredS3Artifacts(ctx, urlTTL, files, repository, jobs)
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				purgeExpiredS3Artifacts(ctx, urlTTL, files, repository, jobs)
			}
		}
	}()
	return func() { <-done }
}

func purgeExpiredS3Artifacts(
	ctx context.Context,
	urlTTL time.Duration,
	files artifactDeleter,
	repository ExpiredArtifactRepository,
	jobs expiredArtifactJobs,
) {
	if urlTTL < 0 {
		urlTTL = 0
	}
	// A URL can be minted until ExpiresAt and remains valid for one more URL TTL.
	cutoff := time.Now().Add(-urlTTL)
	afterID := ""
	for ctx.Err() == nil {
		candidates, err := repository.ListExpiredArtifacts(ctx, cutoff, afterID, expiredArtifactBatchSize)
		if err != nil {
			slog.Warn("failed to list expired S3 artifacts", "error", err)
			return
		}
		if len(candidates) == 0 {
			return
		}
		for _, candidate := range candidates {
			afterID = candidate.ID
			if candidate.ID == "" || candidate.Location == "" ||
				!strings.HasPrefix(candidate.Location, "artifacts/") ||
				candidate.ExpiresAt.After(cutoff) {
				slog.Warn("skipping unsafe expired S3 artifact candidate", "job_id", candidate.ID)
				continue
			}
			if jobs != nil {
				jobs.RemoveExpiredArtifact(candidate.ID, candidate.Location, cutoff)
			}
			if err := files.Delete(ctx, artifact.Saved{Backend: "s3", Location: candidate.Location}); err != nil {
				slog.Warn("failed to delete expired S3 artifact", "job_id", candidate.ID, "error", err)
				continue
			}
			if err := repository.DeleteExpiredArtifact(ctx, candidate, cutoff); err != nil {
				slog.Warn("failed to remove expired artifact record", "job_id", candidate.ID, "error", err)
			}
		}
		if len(candidates) < expiredArtifactBatchSize {
			return
		}
	}
}
