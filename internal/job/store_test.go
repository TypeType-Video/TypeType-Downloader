package job

import (
	"errors"
	"sync"
	"testing"
)

func TestStoreCreateStartProgressDone(t *testing.T) {
	store := NewStore("http://localhost")
	record, _, _, err := store.Create("https://example.com/watch?v=1", Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	store.Start(record.ID, func() {})
	store.Progress(record.ID, Progress{Stage: "download", DownloadedBytes: 50, TotalBytes: 100, SpeedBytesPerSecond: 25})
	store.Done(record.ID, "/tmp/out.mp4", "local", nil, 10, 20)
	response, ok := store.Response(record.ID)
	if !ok {
		t.Fatal("missing response")
	}
	if response.Status != StatusDone {
		t.Fatalf("status = %s, want done", response.Status)
	}
	if response.ArtifactURL == nil || *response.ArtifactURL != "http://localhost/jobs/"+record.ID+"/artifact" {
		t.Fatalf("artifact URL = %#v", response.ArtifactURL)
	}
	if response.ProgressPercent == nil || *response.ProgressPercent != 100 {
		t.Fatalf("progress = %#v, want 100", response.ProgressPercent)
	}
}

func TestStoreCreateIsAtomicForConcurrentDuplicateRequests(t *testing.T) {
	store := NewStore("http://localhost")
	const callers = 32
	start := make(chan struct{})
	results := make(chan *Record, callers)
	created := make(chan bool, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			record, _, wasCreated, err := store.Create("https://example.com/watch?v=duplicate", Options{Container: "mp4"}, "")
			if err != nil {
				t.Error(err)
				return
			}
			results <- record
			created <- wasCreated
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(created)
	var firstID string
	createdCount := 0
	for record := range results {
		if firstID == "" {
			firstID = record.ID
		}
		if record.ID != firstID {
			t.Fatalf("duplicate request created %s and %s", firstID, record.ID)
		}
	}
	for wasCreated := range created {
		if wasCreated {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
}

func TestStoreGetAndResponseReturnSnapshots(t *testing.T) {
	store := NewStore("http://localhost")
	record, _, _, err := store.Create("https://example.com/watch?v=snapshot", Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	message := "original"
	store.Fail(record.ID, "failure", errors.New(message))
	snapshot, ok := store.Get(record.ID)
	if !ok || snapshot.Error == nil {
		t.Fatal("missing error snapshot")
	}
	*snapshot.Error = "changed"
	response, ok := store.Response(record.ID)
	if !ok || response.Error == nil || *response.Error != message {
		t.Fatalf("response error = %#v", response.Error)
	}
}

func TestStoreCancelQueuedMarksFailed(t *testing.T) {
	store := NewStore("http://localhost")
	record, _, _, err := store.Create("https://example.com/watch?v=1", Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !store.Cancel(record.ID) {
		t.Fatal("Cancel() returned false")
	}
	response, _ := store.Response(record.ID)
	if response.Status != StatusFailed || response.ErrorCode == nil || *response.ErrorCode != "cancelled" {
		t.Fatalf("response = %#v", response)
	}
}

func TestStoreFailPublishesError(t *testing.T) {
	store := NewStore("http://localhost")
	record, _, _, err := store.Create("https://example.com/watch?v=1", Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	store.Fail(record.ID, "boom", errors.New("failed"))
	response, _ := store.Response(record.ID)
	if response.Status != StatusFailed || response.Error == nil || *response.Error != "failed" {
		t.Fatalf("response = %#v", response)
	}
}

func TestStoreDeduplicatesDoneJobs(t *testing.T) {
	store := NewStore("http://localhost")
	first, cached, created, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if cached || !created {
		t.Fatalf("first create cached=%v created=%v", cached, created)
	}
	store.Done(first.ID, "/tmp/out.mp4", "local", nil, 10, 20)
	second, cached, created, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || !cached || created {
		t.Fatalf("second id=%s cached=%v created=%v", second.ID, cached, created)
	}
}

func TestStoreDeduplicatesRunningJobsWithoutCachedFlag(t *testing.T) {
	store := NewStore("http://localhost")
	first, _, _, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "")
	if err != nil {
		t.Fatal(err)
	}
	store.Start(first.ID, func() {})
	second, cached, created, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || cached || created {
		t.Fatalf("second id=%s cached=%v created=%v", second.ID, cached, created)
	}
}

func TestStoreAuthorizationScopesCacheKey(t *testing.T) {
	store := NewStore("http://localhost")
	first, _, firstCreated, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "Bearer one")
	if err != nil {
		t.Fatal(err)
	}
	second, _, secondCreated, err := store.Create("https://example.com/watch?v=1", Options{Container: "mp4"}, "Bearer two")
	if err != nil {
		t.Fatal(err)
	}
	if !firstCreated || !secondCreated || first.ID == second.ID {
		t.Fatalf("first=%s created=%v second=%s created=%v", first.ID, firstCreated, second.ID, secondCreated)
	}
	if first.Authorization != "Bearer one" || second.Authorization != "Bearer two" {
		t.Fatalf("authorization was not retained in memory")
	}
}
