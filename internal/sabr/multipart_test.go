package sabr

import (
	"context"
	"net/http"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestDownloadBoundsConcurrentSABRParts(t *testing.T) {
	var active atomic.Int64
	var peak atomic.Int64
	var requests atomic.Int64
	server := downloadTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		requests.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		part, err := strconv.Atoi(request.URL.Query().Get("part"))
		if err != nil {
			t.Error(err)
			return
		}
		writeTestFrame(t, writer, frameInitialization, 140, 0, []byte("init"))
		writeTestFrame(t, writer, frameMedia, 140, part+1, []byte("media"))
		writeTestFrame(t, writer, frameComplete, 0, 0, nil)
	})
	defer server.Close()

	output := filepath.Join(t.TempDir(), "audio.m4a")
	err := Download(context.Background(), server.Client(), Options{
		ManifestURL: server.URL,
		AudioItag:   140,
		AudioOnly:   true,
		AudioPath:   output,
		Parts:       maxDownloadParts,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != maxDownloadParts {
		t.Fatalf("requests = %d, want %d", requests.Load(), maxDownloadParts)
	}
	if peak.Load() != maxConcurrentPartDownloads {
		t.Fatalf("peak concurrency = %d, want %d", peak.Load(), maxConcurrentPartDownloads)
	}
}

func TestDownloadRetriesOnlyTheFailedPart(t *testing.T) {
	var firstPart, secondPart atomic.Int64
	server := downloadTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("part") == "0" {
			firstPart.Add(1)
			writeTestFrame(t, writer, frameInitialization, 140, 0, []byte("ai"))
			writeTestFrame(t, writer, frameMedia, 140, 1, []byte("a1"))
			writeTestFrame(t, writer, frameComplete, 0, 0, nil)
			return
		}
		writeTestFrame(t, writer, frameInitialization, 140, 0, []byte("ai"))
		writeTestFrame(t, writer, frameMedia, 140, 2, []byte("a2"))
		if secondPart.Add(1) == 1 {
			return
		}
		writeTestFrame(t, writer, frameComplete, 0, 0, nil)
	})
	defer server.Close()

	output := filepath.Join(t.TempDir(), "audio.m4a")
	var progress int64
	err := Download(context.Background(), server.Client(), Options{
		ManifestURL: server.URL, AudioItag: 140, AudioOnly: true, AudioPath: output, Parts: 2,
	}, func(value int64) { progress = value })
	if err != nil {
		t.Fatal(err)
	}
	assertFile(t, output, "aia1a2")
	if firstPart.Load() != 1 || secondPart.Load() != 2 {
		t.Fatalf("requests = %d/%d, want 1/2", firstPart.Load(), secondPart.Load())
	}
	if progress != 6 {
		t.Fatalf("progress = %d, want 6", progress)
	}
}

func TestDownloadFallsBackToOneStreamWhenAPartKeepsFailing(t *testing.T) {
	var singleStream atomic.Int64
	server := downloadTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if query.Get("parts") == "1" {
			singleStream.Add(1)
			writeTestFrame(t, writer, frameInitialization, 140, 0, []byte("ai"))
			writeTestFrame(t, writer, frameMedia, 140, 1, []byte("a1"))
			writeTestFrame(t, writer, frameMedia, 140, 2, []byte("a2"))
			writeTestFrame(t, writer, frameComplete, 0, 0, nil)
			return
		}
		if query.Get("part") == "1" {
			return
		}
		writeTestFrame(t, writer, frameInitialization, 140, 0, []byte("ai"))
		writeTestFrame(t, writer, frameMedia, 140, 1, []byte("a1"))
		writeTestFrame(t, writer, frameComplete, 0, 0, nil)
	})
	defer server.Close()

	output := filepath.Join(t.TempDir(), "audio.m4a")
	err := Download(context.Background(), server.Client(), Options{
		ManifestURL: server.URL, AudioItag: 140, AudioOnly: true, AudioPath: output, Parts: 2,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertFile(t, output, "aia1a2")
	if singleStream.Load() != 1 {
		t.Fatalf("single stream requests = %d, want 1", singleStream.Load())
	}
	assertNoFile(t, partPath(output, 0))
	assertNoFile(t, partPath(output, 1))
}
