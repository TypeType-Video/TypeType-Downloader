package cleanup

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"typetype-downloader-go/internal/artifact"
)

type artifactDeleteStub struct {
	deleted []artifact.Saved
	err     error
}

func (s *artifactDeleteStub) Delete(_ context.Context, saved artifact.Saved) error {
	s.deleted = append(s.deleted, saved)
	return s.err
}

type expiredArtifactRepositoryStub struct {
	items   []ExpiredArtifact
	deleted []ExpiredArtifact
	err     error
}

func (s *expiredArtifactRepositoryStub) ListExpiredArtifacts(_ context.Context, before time.Time, afterID string, limit int) ([]ExpiredArtifact, error) {
	items := make([]ExpiredArtifact, 0, len(s.items))
	for _, item := range s.items {
		if item.ID > afterID && !item.ExpiresAt.After(before) {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *expiredArtifactRepositoryStub) DeleteExpiredArtifact(_ context.Context, item ExpiredArtifact, _ time.Time) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, item)
	return nil
}

type expiredArtifactJobsStub struct {
	removed []ExpiredArtifact
}

func (s *expiredArtifactJobsStub) RemoveExpiredArtifact(id string, location string, expiresAt time.Time) bool {
	s.removed = append(s.removed, ExpiredArtifact{ID: id, Location: location, ExpiresAt: expiresAt})
	return true
}

func TestPurgeExpiredS3ArtifactsWaitsForSignedURLsToExpire(t *testing.T) {
	now := time.Now()
	expired := ExpiredArtifact{ID: "old", Location: "artifacts/old/video.mp4", ExpiresAt: now.Add(-3 * time.Hour)}
	repository := &expiredArtifactRepositoryStub{items: []ExpiredArtifact{
		expired,
		{ID: "recent", Location: "artifacts/recent/video.mp4", ExpiresAt: now.Add(-time.Hour)},
		{ID: "future", Location: "artifacts/future/video.mp4", ExpiresAt: now.Add(time.Minute)},
		{ID: "other", Location: "profiles/avatar.jpg", ExpiresAt: now.Add(-3 * time.Hour)},
	}}
	files := &artifactDeleteStub{}
	jobs := &expiredArtifactJobsStub{}

	purgeExpiredS3Artifacts(context.Background(), 2*time.Hour, files, repository, jobs)

	if len(files.deleted) != 1 || files.deleted[0].Location != expired.Location {
		t.Fatalf("deleted objects = %+v", files.deleted)
	}
	if len(repository.deleted) != 1 || repository.deleted[0].ID != expired.ID {
		t.Fatalf("deleted database records = %+v", repository.deleted)
	}
	if len(jobs.removed) != 1 || jobs.removed[0].ID != expired.ID {
		t.Fatalf("removed in-memory jobs = %+v", jobs.removed)
	}
}

func TestPurgeKeepsDatabaseRecordWhenObjectDeleteFails(t *testing.T) {
	expired := ExpiredArtifact{ID: "old", Location: "artifacts/old/video.mp4", ExpiresAt: time.Now().Add(-3 * time.Hour)}
	repository := &expiredArtifactRepositoryStub{items: []ExpiredArtifact{expired}}
	files := &artifactDeleteStub{err: errors.New("garage unavailable")}

	purgeExpiredS3Artifacts(context.Background(), 2*time.Hour, files, repository, nil)

	if len(repository.deleted) != 0 {
		t.Fatalf("deleted database records after object failure = %+v", repository.deleted)
	}
}
