package api

import (
	"net/http"
	"time"

	"typetype-downloader-go/internal/artifact"
	"typetype-downloader-go/internal/job"
)

func (s *Server) artifact(w http.ResponseWriter, r *http.Request, id string) {
	record, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if record.Status != job.StatusDone || record.Artifact == "" {
		writeError(w, http.StatusConflict, "artifact_not_ready", "artifact is not ready")
		return
	}
	if record.ExpiresAt != nil && !record.ExpiresAt.After(time.Now()) {
		writeError(w, http.StatusNotFound, "not_found", "artifact expired")
		return
	}
	fileName := ""
	if record.Resolved != nil && record.Resolved.FileName != "" {
		fileName = record.Resolved.FileName
		w.Header().Set("Content-Disposition", `attachment; filename="`+fileName+`"`)
	}
	saved := artifact.Saved{Backend: record.Storage, Location: record.Artifact}
	if err := s.files.ServeHTTP(w, r, saved, fileName); err != nil {
		writeError(w, http.StatusNotFound, "artifact_missing", "artifact missing")
	}
}
