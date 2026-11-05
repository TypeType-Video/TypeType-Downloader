package sabr

import (
	"sync"
	"sync/atomic"
	"time"
)

type byteCounter interface {
	add(bytes int64)
}

type partProgress struct {
	parent *reporter
	bytes  int64
}

func (p *partProgress) add(bytes int64) {
	p.bytes += bytes
	p.parent.add(bytes)
}

func (p *partProgress) rollback() {
	p.parent.add(-p.bytes)
	p.bytes = 0
}

type reporter struct {
	downloaded atomic.Int64
	progress   ProgressFunc
	reportMu   sync.Mutex
	reported   int64
	last       time.Time
}

func newReporter(progress ProgressFunc) *reporter {
	return &reporter{progress: progress, last: time.Now()}
}

func (r *reporter) beginAttempt() {
	r.downloaded.Store(0)
	r.reportMu.Lock()
	r.last = time.Now()
	r.reportMu.Unlock()
}

func (r *reporter) add(bytes int64) {
	downloaded := r.downloaded.Add(bytes)
	if r.progress == nil {
		return
	}
	now := time.Now()
	r.reportMu.Lock()
	defer r.reportMu.Unlock()
	if downloaded <= r.reported || now.Sub(r.last) < 250*time.Millisecond {
		return
	}
	r.reported = downloaded
	r.last = now
	r.progress(downloaded)
}

func (r *reporter) finish() {
	if r.progress == nil {
		return
	}
	r.reportMu.Lock()
	defer r.reportMu.Unlock()
	downloaded := r.downloaded.Load()
	if downloaded <= r.reported {
		return
	}
	r.reported = downloaded
	r.progress(downloaded)
}
