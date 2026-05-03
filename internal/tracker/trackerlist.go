package tracker

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// TrackerList manages the list of upstream trackers (static + remote).
type TrackerList struct {
	mu          sync.RWMutex
	trackers    []string
	static      []string
	remoteURL   string
	refreshEvery time.Duration
	fetchTimeout time.Duration
	log         *slog.Logger
}

func NewTrackerList(static []string, remoteURL string, refresh, timeout time.Duration, log *slog.Logger) *TrackerList {
	tl := &TrackerList{
		static:       static,
		remoteURL:    remoteURL,
		refreshEvery: refresh,
		fetchTimeout: timeout,
		log:          log,
	}
	tl.trackers = append([]string{}, static...)
	return tl
}

// All returns the current list of tracker URLs.
func (tl *TrackerList) All() []string {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	out := make([]string, len(tl.trackers))
	copy(out, tl.trackers)
	return out
}

// Count returns the number of known trackers.
func (tl *TrackerList) Count() int {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	return len(tl.trackers)
}

// Start begins the background refresh loop.
func (tl *TrackerList) Start(ctx context.Context) {
	if tl.remoteURL == "" {
		tl.log.Info("No remote tracker list URL configured, using static list only",
			"count", len(tl.static))
		return
	}
	// Fetch immediately, then on a timer
	tl.fetch(ctx)
	go func() {
		ticker := time.NewTicker(tl.refreshEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tl.fetch(ctx)
			}
		}
	}()
}

func (tl *TrackerList) fetch(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, tl.fetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tl.remoteURL, nil)
	if err != nil {
		tl.log.Warn("Building tracker list request failed", "err", err)
		return
	}
	req.Header.Set("User-Agent", "retracker/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		tl.log.Warn("Fetching remote tracker list failed", "url", tl.remoteURL, "err", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB max
	if err != nil {
		tl.log.Warn("Reading remote tracker list failed", "err", err)
		return
	}

	remote := parseTrackerList(string(body))
	tl.log.Info("Fetched remote tracker list", "url", tl.remoteURL, "count", len(remote))

	// Merge static + remote, deduplicate
	seen := make(map[string]struct{}, len(tl.static)+len(remote))
	merged := make([]string, 0, len(tl.static)+len(remote))
	for _, t := range append(tl.static, remote...) {
		if _, ok := seen[t]; !ok && t != "" {
			seen[t] = struct{}{}
			merged = append(merged, t)
		}
	}

	tl.mu.Lock()
	tl.trackers = merged
	tl.mu.Unlock()
}

// parseTrackerList parses a newline-separated list of tracker URLs,
// ignoring blank lines and comments.
func parseTrackerList(body string) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Accept http://, https://, udp://
		if strings.HasPrefix(line, "http://") ||
			strings.HasPrefix(line, "https://") ||
			strings.HasPrefix(line, "udp://") {
			out = append(out, line)
		}
	}
	return out
}
