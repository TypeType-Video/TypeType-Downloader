package job

import "time"

func (s *Store) Response(id string) (Response, bool) {
	s.mu.RLock()
	record, ok := s.jobs[id]
	if !ok {
		s.mu.RUnlock()
		return Response{}, false
	}
	response := s.toResponse(record)
	s.mu.RUnlock()
	return response, true
}

func (s *Store) toResponse(record *Record) Response {
	response := Response{
		ID:         record.ID,
		URL:        record.URL,
		Status:     record.Status,
		Title:      record.Title,
		Error:      cloneString(record.Error),
		ErrorCode:  cloneString(record.ErrorCode),
		Resolved:   cloneResolved(record.Resolved),
		QueuedAt:   formatTime(record.QueuedAt),
		StartedAt:  formatTimePtr(record.StartedAt),
		FinishedAt: formatTimePtr(record.FinishedAt),
		DownloadMs: cloneInt64(record.DownloadMs),
		MuxMs:      cloneInt64(record.MuxMs),
		TotalMs:    cloneInt64(record.TotalMs),
	}
	if record.Artifact != "" {
		artifactURL := s.baseURL + "/jobs/" + record.ID + "/artifact"
		response.ArtifactURL = &artifactURL
	}
	response.ArtifactExpiresAt = formatTimePtr(record.ExpiresAt)
	if record.StartedAt != nil {
		queueWait := record.StartedAt.Sub(record.QueuedAt).Milliseconds()
		response.QueueWaitMs = &queueWait
	}
	if record.StartedAt != nil {
		end := time.Now()
		if record.FinishedAt != nil {
			end = *record.FinishedAt
		}
		runTime := end.Sub(*record.StartedAt).Milliseconds()
		response.RunTimeMs = &runTime
	}
	applyProgress(&response, record.Progress)
	return response
}

func applyProgress(response *Response, progress Progress) {
	if progress.Stage != "" {
		response.Stage = &progress.Stage
	}
	if progress.TotalBytes > 0 {
		percent := int(progress.DownloadedBytes * 100 / progress.TotalBytes)
		response.ProgressPercent = &percent
		response.TotalBytes = &progress.TotalBytes
		response.DownloadedBytes = &progress.DownloadedBytes
	}
	if progress.SpeedBytesPerSecond > 0 {
		response.SpeedBytesPerSecond = &progress.SpeedBytesPerSecond
	}
}

func formatTime(value time.Time) *string {
	formatted := value.UTC().Format(time.RFC3339Nano)
	return &formatted
}

func formatTimePtr(value *time.Time) *string {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}
