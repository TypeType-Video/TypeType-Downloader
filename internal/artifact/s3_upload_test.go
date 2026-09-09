package artifact

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestS3StoreUploadsExactFileAndDeletesArtifact(t *testing.T) {
	payload := bytes.Repeat([]byte("artifact-content"), 8192)
	uploads := make(chan []byte, 1)
	deletes := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/downloads/artifact.mp4" {
			t.Errorf("unexpected object path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("missing signed request")
		}
		switch r.Method {
		case http.MethodPut:
			uploaded, err := io.ReadAll(io.LimitReader(r.Body, int64(len(payload)+1)))
			if err != nil {
				t.Error(err)
			}
			if r.Header.Get("Content-Type") != "video/mp4" {
				t.Errorf("content type: %s", r.Header.Get("Content-Type"))
			}
			w.Header().Set("ETag", `"test-artifact"`)
			uploads <- uploaded
		case http.MethodDelete:
			deletes <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method: %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	endpoint := strings.TrimPrefix(server.URL, "https://")
	store, err := NewS3Store(S3Config{
		Endpoint: endpoint, Region: "test", Bucket: "downloads",
		AccessKey: "test-key", SecretKey: "test-secret", UseSSL: true, PathStyle: true,
		URLTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.client, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("test-key", "test-secret", ""),
		Secure: true, Region: "test", BucketLookup: minio.BucketLookupPath,
		Transport: server.Client().Transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "artifact.mp4")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Save(t.Context(), path, "artifact.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-uploads, payload) || saved.Backend != "s3" || saved.Location != "artifact.mp4" ||
		!saved.Expires.After(time.Now()) {
		t.Fatal("artifact bytes or metadata changed during upload")
	}
	if err := store.Delete(t.Context(), saved); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deletes:
	default:
		t.Fatal("artifact was not deleted")
	}
}
