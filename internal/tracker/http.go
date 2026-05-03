// Package tracker implements the HTTP (BEP 3 / BEP 23) and UDP (BEP 15) tracker servers.
package tracker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/L2jLiga/retracker/internal/announce"
	"github.com/L2jLiga/retracker/internal/metrics"
	"github.com/L2jLiga/retracker/internal/peers"
)

// HTTPTracker handles BEP 3 announce and scrape requests.
type HTTPTracker struct {
	store    *peers.Store
	interval time.Duration
	upstream UpstreamNotifier
	log      *slog.Logger
}

// UpstreamNotifier is called when a new announce is received so the upstream
// re-announcer can pick it up.
type UpstreamNotifier interface {
	Notify(ih peers.InfoHash, p *peers.Peer)
}

func NewHTTPTracker(store *peers.Store, interval time.Duration, up UpstreamNotifier, log *slog.Logger) *HTTPTracker {
	return &HTTPTracker{store: store, interval: interval, upstream: up, log: log}
}

func (h *HTTPTracker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", h.handleAnnounce)
	mux.HandleFunc("/scrape", h.handleScrape)
	return mux
}

// handleAnnounce implements BEP 3 + BEP 23 (compact peers).
func (h *HTTPTracker) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	metrics.HTTPRequests.WithLabelValues("announce").Inc()

	q := r.URL.Query()

	ih, err := parseInfoHash(q.Get("info_hash"))
	if err != nil {
		writeBencodeError(w, "invalid info_hash")
		return
	}

	pid, err := parsePeerID(q.Get("peer_id"))
	if err != nil {
		writeBencodeError(w, "invalid peer_id")
		return
	}

	port, err := strconv.ParseUint(q.Get("port"), 10, 16)
	if err != nil || port == 0 {
		writeBencodeError(w, "invalid port")
		return
	}

	uploaded, _ := strconv.ParseInt(q.Get("uploaded"), 10, 64)
	downloaded, _ := strconv.ParseInt(q.Get("downloaded"), 10, 64)
	left, _ := strconv.ParseInt(q.Get("left"), 10, 64)
	event := q.Get("event")
	numWant := 50
	if nw := q.Get("numwant"); nw != "" {
		if n, e := strconv.Atoi(nw); e == nil {
			numWant = n
		}
	}
	compact := q.Get("compact") != "0" // default compact=1 (BEP 23)
	nopeerId := q.Get("no_peer_id") == "1"

	ip := peerIP(r, q.Get("ip"))

	peer := &peers.Peer{
		ID:         pid,
		IP:         ip,
		Port:       uint16(port),
		Uploaded:   uploaded,
		Downloaded: downloaded,
		Left:       left,
		Event:      event,
		LastSeen:   time.Now(),
	}

	peerList := h.store.Announce(ih, peer, numWant)
	metrics.PeersAnnounced.Inc()

	// Notify upstream re-announcer (non-blocking)
	if h.upstream != nil && event != "stopped" {
		h.upstream.Notify(ih, peer)
	}

	// Build response
	resp := map[string]interface{}{
		"interval":     int64(h.interval.Seconds()),
		"min interval": int64(h.interval.Seconds() / 2),
	}

	seeders, leechers := h.swarmStats(ih)
	resp["complete"] = int32(seeders)
	resp["incomplete"] = int32(leechers)

	if compact {
		// BEP 23: separate compact IPv4 / IPv6 peer lists
		ipv4Peers := make([]*peers.Peer, 0)
		ipv6Peers := make([]*peers.Peer, 0)
		for _, p := range peerList {
			if p.IP.To4() != nil {
				ipv4Peers = append(ipv4Peers, p)
			} else {
				ipv6Peers = append(ipv6Peers, p)
			}
		}
		resp["peers"] = peers.CompactIPv4(ipv4Peers)
		if len(ipv6Peers) > 0 {
			resp["peers6"] = peers.CompactIPv6(ipv6Peers)
		}
	} else {
		// Dict peers (legacy)
		list := make([]interface{}, 0, len(peerList))
		for _, p := range peerList {
			entry := map[string]interface{}{
				"ip":   p.IP.String(),
				"port": int64(p.Port),
			}
			if !nopeerId {
				entry["peer id"] = p.ID[:]
			}
			list = append(list, entry)
		}
		resp["peers"] = list
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(announce.BencodeDict(resp))
}

// handleScrape implements BEP 48 scrape.
func (h *HTTPTracker) handleScrape(w http.ResponseWriter, r *http.Request) {
	metrics.HTTPRequests.WithLabelValues("scrape").Inc()

	// Multiple info_hash params
	q := r.URL.Query()
	rawHashes := q["info_hash"]
	hashes := make([]peers.InfoHash, 0, len(rawHashes))
	for _, raw := range rawHashes {
		ih, err := parseInfoHash(raw)
		if err == nil {
			hashes = append(hashes, ih)
		}
	}
	if len(hashes) == 0 {
		writeBencodeError(w, "no valid info_hash")
		return
	}

	stats := h.store.Scrape(hashes)
	files := make(map[string]interface{}, len(stats))
	for ih, s := range stats {
		files[string(ih[:])] = map[string]interface{}{
			"complete":   s[0],
			"incomplete": s[1],
			"downloaded": s[2],
		}
	}
	resp := map[string]interface{}{"files": files}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(announce.BencodeDict(resp))
}

func (h *HTTPTracker) swarmStats(ih peers.InfoHash) (int, int) {
	stats := h.store.Scrape([]peers.InfoHash{ih})
	if s, ok := stats[ih]; ok {
		return int(s[0]), int(s[1])
	}
	return 0, 0
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// parseInfoHash extracts a 20-byte info_hash from a URL query parameter.
// The BitTorrent HTTP protocol percent-encodes the raw 20-byte value.
func parseInfoHash(raw string) (peers.InfoHash, error) {
	decoded, err := url.QueryUnescape(raw)
	if err != nil {
		return peers.InfoHash{}, fmt.Errorf("url decode: %w", err)
	}
	if len(decoded) != 20 {
		return peers.InfoHash{}, fmt.Errorf("info_hash must be 20 bytes, got %d", len(decoded))
	}
	var ih peers.InfoHash
	copy(ih[:], decoded)
	return ih, nil
}

func parsePeerID(raw string) (peers.PeerID, error) {
	decoded, err := url.QueryUnescape(raw)
	if err != nil {
		return peers.PeerID{}, fmt.Errorf("url decode: %w", err)
	}
	if len(decoded) != 20 {
		return peers.PeerID{}, fmt.Errorf("peer_id must be 20 bytes, got %d", len(decoded))
	}
	var pid peers.PeerID
	copy(pid[:], decoded)
	return pid, nil
}

func peerIP(r *http.Request, override string) net.IP {
	if override != "" {
		if ip := net.ParseIP(override); ip != nil {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// X-Forwarded-For (trusted internal network only)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := net.ParseIP(strings.TrimSpace(parts[0])); ip != nil {
			return ip
		}
	}
	return net.ParseIP(host)
}

func writeBencodeError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK) // BEP 3: errors still return 200
	_, _ = w.Write(announce.BencodeError(msg))
}

// ── HTTP Server lifecycle ────────────────────────────────────────────────────

type HTTPServer struct {
	srv *http.Server
	log *slog.Logger
}

func NewHTTPServer(addr string, handler http.Handler, log *slog.Logger) *HTTPServer {
	return &HTTPServer{
		srv: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		log: log,
	}
}

func (s *HTTPServer) Start() error {
	s.log.Info("HTTP tracker listening", "addr", s.srv.Addr)
	return s.srv.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
