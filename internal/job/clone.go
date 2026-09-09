package job

func cloneRecord(record *Record) *Record {
	copy := *record
	copy.Error = cloneString(record.Error)
	copy.ErrorCode = cloneString(record.ErrorCode)
	copy.StartedAt = cloneTime(record.StartedAt)
	copy.FinishedAt = cloneTime(record.FinishedAt)
	copy.ExpiresAt = cloneTime(record.ExpiresAt)
	copy.DownloadMs = cloneInt64(record.DownloadMs)
	copy.MuxMs = cloneInt64(record.MuxMs)
	copy.TotalMs = cloneInt64(record.TotalMs)
	if record.Resolved != nil {
		copy.Resolved = cloneResolved(record.Resolved)
	}
	return &copy
}

func cloneResolved(value *ResolvedOutput) *ResolvedOutput {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
