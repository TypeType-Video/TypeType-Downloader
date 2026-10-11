package db

import (
	"context"
	"fmt"
	"time"

	"typetype-downloader-go/internal/cleanup"
)

const listExpiredArtifactsSQL = "select id, artifact, artifact_expires_at " +
	"from downloader_jobs " +
	"where status = 'done' and storage = 's3' and artifact <> '' " +
	"and artifact like 'artifacts/%' and artifact_expires_at <= $1 and id > $2 " +
	"order by id limit $3"

const deleteExpiredArtifactSQL = "delete from downloader_jobs " +
	"where id = $1 and status = 'done' and storage = 's3' and artifact = $2 " +
	"and artifact_expires_at = $3 and artifact_expires_at <= $4"

func (s *PostgresSink) ListExpiredArtifacts(
	ctx context.Context,
	before time.Time,
	afterID string,
	limit int,
) ([]cleanup.ExpiredArtifact, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, listExpiredArtifactsSQL, before, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired artifacts: %w", err)
	}
	defer rows.Close()
	var candidates []cleanup.ExpiredArtifact
	for rows.Next() {
		var candidate cleanup.ExpiredArtifact
		if err := rows.Scan(&candidate.ID, &candidate.Location, &candidate.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan expired artifact: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read expired artifacts: %w", err)
	}
	return candidates, nil
}

func (s *PostgresSink) DeleteExpiredArtifact(ctx context.Context, candidate cleanup.ExpiredArtifact, before time.Time) error {
	_, err := s.pool.Exec(ctx, deleteExpiredArtifactSQL,
		candidate.ID, candidate.Location, candidate.ExpiresAt, before)
	if err != nil {
		return fmt.Errorf("delete expired artifact record %s: %w", candidate.ID, err)
	}
	return nil
}
