package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"typetype-downloader-go/internal/artifact"
	"typetype-downloader-go/internal/job"
)

type artifactExpiryStore struct {
	served int
}

func (s *artifactExpiryStore) Name() string                 { return "test" }
func (s *artifactExpiryStore) Health(context.Context) error { return nil }
func (s *artifactExpiryStore) Save(context.Context, string, string) (artifact.Saved, error) {
	return artifact.Saved{}, nil
}
func (s *artifactExpiryStore) ServeHTTP(http.ResponseWriter, *http.Request, artifact.Saved, string) error {
	s.served++
	return nil
}
func (s *artifactExpiryStore) Delete(context.Context, artifact.Saved) error { return nil }

func TestExpiredArtifactCannotBeServedAgain(t *testing.T) {
	store := job.NewStore("http://localhost")
	record, _, _, err := store.Create("https://example.com/watch?v=expired-artifact", job.Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(-time.Second)
	store.Done(record.ID, "artifacts/"+record.ID+"/video.mp4", "s3", &expires, 10, 20)
	files := &artifactExpiryStore{}
	server := &Server{store: store, files: files}
	request := httptest.NewRequest(http.MethodGet, "/jobs/"+record.ID+"/artifact", nil)
	response := httptest.NewRecorder()

	server.artifact(response, request, record.ID)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if files.served != 0 {
		t.Fatalf("expired artifact was passed to storage %d times", files.served)
	}
}
