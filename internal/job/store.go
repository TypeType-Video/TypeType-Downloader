package job

import (
	"sync"
	"time"
)

type Store struct {
	mu        sync.RWMutex
	baseURL   string
	jobs      map[string]*Record
	cache     map[string]string
	listeners map[string]map[chan Response]struct{}
	sinks     []Sink
}

type Sink interface {
	SaveJob(*Record)
}

func NewStore(baseURL string, sinks ...Sink) *Store {
	return &Store{
		baseURL:   baseURL,
		jobs:      map[string]*Record{},
		cache:     map[string]string{},
		listeners: map[string]map[chan Response]struct{}{},
		sinks:     sinks,
	}
}

func (s *Store) Restore(records []*Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range records {
		if record == nil || record.ID == "" {
			continue
		}
		restored := cloneRecord(record)
		restored.Cancel = nil
		s.jobs[restored.ID] = restored
		if restored.CacheKey != "" && restored.Status == StatusDone {
			s.cache[restored.CacheKey] = restored.ID
		}
	}
}

func (s *Store) RestorePending(records []*Record) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if record == nil || record.ID == "" {
			continue
		}
		restored := cloneRecord(record)
		restored.Status = StatusQueued
		restored.Cancel = nil
		restored.StartedAt = nil
		restored.FinishedAt = nil
		restored.Error = nil
		restored.ErrorCode = nil
		restored.Progress = Progress{Stage: "queued"}
		s.jobs[restored.ID] = restored
		ids = append(ids, restored.ID)
	}
	return ids
}

func (s *Store) Create(rawURL string, options Options, authorization string) (*Record, bool, bool, error) {
	cacheKey, err := CacheKey(rawURL, options, authorization)
	if err != nil {
		return nil, false, false, err
	}
	id, err := newID()
	if err != nil {
		return nil, false, false, err
	}
	record := &Record{ID: id, CacheKey: cacheKey, URL: rawURL, Authorization: authorization, Options: options, Status: StatusQueued, QueuedAt: time.Now()}
	s.mu.Lock()
	if existingID := s.cache[cacheKey]; existingID != "" {
		if existing := s.jobs[existingID]; existing != nil && existing.Status != StatusFailed {
			cached := existing.Status == StatusDone
			snapshot := cloneRecord(existing)
			s.mu.Unlock()
			return snapshot, cached, false, nil
		}
	}
	s.jobs[id] = record
	s.cache[cacheKey] = id
	s.mu.Unlock()
	s.broadcast(id)
	s.notify(record)
	return cloneRecord(record), false, true, nil
}

func (s *Store) Get(id string) (*Record, bool) {
	s.mu.RLock()
	record, ok := s.jobs[id]
	if ok {
		record = cloneRecord(record)
	}
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return record, true
}

func (s *Store) Start(id string, cancel func()) bool {
	now := time.Now()
	updated := s.update(id, func(record *Record) {
		record.Status = StatusRunning
		record.StartedAt = &now
		record.Cancel = cancel
		record.Progress.Stage = "extract"
	})
	return updated
}

func (s *Store) Progress(id string, progress Progress) {
	s.update(id, func(record *Record) { record.Progress = progress })
}

func (s *Store) Resolve(id string, title string, resolved ResolvedOutput) {
	s.update(id, func(record *Record) {
		record.Title = title
		record.Resolved = &resolved
	})
}

func (s *Store) Done(id string, artifact string, storage string, expiresAt *time.Time, downloadMs int64, muxMs int64) {
	now := time.Now()
	totalMs := int64(0)
	s.update(id, func(record *Record) {
		record.Status = StatusDone
		record.Artifact = artifact
		record.Storage = storage
		record.ExpiresAt = expiresAt
		record.FinishedAt = &now
		record.DownloadMs = &downloadMs
		record.MuxMs = &muxMs
		if record.StartedAt != nil {
			totalMs = now.Sub(*record.StartedAt).Milliseconds()
			record.Progress.Stage = "done"
		}
		record.Cancel = nil
		record.Progress.DownloadedBytes = record.Progress.TotalBytes
		record.TotalMs = &totalMs
	})
}

func (s *Store) Fail(id string, code string, err error) {
	now := time.Now()
	message := err.Error()
	s.update(id, func(record *Record) {
		record.Status = StatusFailed
		record.Error = &message
		record.ErrorCode = &code
		record.FinishedAt = &now
		record.Cancel = nil
	})
}

func (s *Store) Cancel(id string) bool {
	var cancel func()
	accepted := false
	updated := s.update(id, func(record *Record) {
		if record.Status == StatusQueued {
			accepted = true
			message := "job cancelled"
			code := "cancelled"
			now := time.Now()
			record.Status = StatusFailed
			record.Error = &message
			record.ErrorCode = &code
			record.FinishedAt = &now
			return
		}
		if record.Status == StatusRunning {
			accepted = true
			cancel = record.Cancel
		}
	})
	if updated && accepted && cancel != nil {
		cancel()
	}
	return updated && accepted
}

func (s *Store) Delete(id string) (*Record, bool) {
	s.mu.Lock()
	record, ok := s.jobs[id]
	if ok && record.Status != StatusRunning {
		delete(s.jobs, id)
		if record.CacheKey != "" && s.cache[record.CacheKey] == id {
			delete(s.cache, record.CacheKey)
		}
	}
	s.mu.Unlock()
	if !ok || record.Status == StatusRunning {
		return nil, false
	}
	s.broadcast(id)
	return cloneRecord(record), true
}
