package job

import "time"

func (s *Store) RemoveExpiredArtifact(id string, location string, now time.Time) bool {
	s.mu.Lock()
	record, ok := s.jobs[id]
	if !ok || record.Status != StatusDone || record.Artifact != location || !artifactExpired(record, now) {
		s.mu.Unlock()
		return false
	}
	delete(s.jobs, id)
	if record.CacheKey != "" && s.cache[record.CacheKey] == id {
		delete(s.cache, record.CacheKey)
	}
	s.mu.Unlock()
	s.broadcast(id)
	return true
}

func artifactExpired(record *Record, now time.Time) bool {
	return record.ExpiresAt != nil && !record.ExpiresAt.After(now)
}
