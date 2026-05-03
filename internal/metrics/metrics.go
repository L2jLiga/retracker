// Package metrics defines all Prometheus metrics for retracker.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "http_requests_total",
		Help:      "Total HTTP tracker requests by type (announce/scrape).",
	}, []string{"type"})

	UDPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "udp_requests_total",
		Help:      "Total UDP tracker requests by action.",
	}, []string{"action"})

	PeersAnnounced = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "peers_announced_total",
		Help:      "Total peer announces processed.",
	})

	ActiveSwarms = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "retracker",
		Name:      "active_swarms",
		Help:      "Number of active swarms (info_hashes with at least one peer).",
	}, func() float64 { return swarmCountFn() })

	ActivePeers = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "retracker",
		Name:      "active_peers",
		Help:      "Total number of active peers across all swarms.",
	}, func() float64 { return peerCountFn() })

	TrackerCount = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "retracker",
		Name:      "upstream_trackers",
		Help:      "Number of known upstream tracker URLs.",
	}, func() float64 { return trackerCountFn() })

	UpstreamAnnounces = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "upstream_announces_total",
		Help:      "Successful upstream announce requests by tracker URL.",
	}, []string{"tracker"})

	UpstreamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "upstream_errors_total",
		Help:      "Failed upstream announce requests by tracker URL.",
	}, []string{"tracker"})

	GCEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "retracker",
		Name:      "gc_evictions_total",
		Help:      "Total peers evicted by GC.",
	})
)

// Callback functions — set during startup.
var (
	swarmCountFn   = func() float64 { return 0 }
	peerCountFn    = func() float64 { return 0 }
	trackerCountFn = func() float64 { return 0 }
)

// RegisterCallbacks wires dynamic gauge callbacks.
func RegisterCallbacks(swarms, peers, trackers func() float64) {
	swarmCountFn = swarms
	peerCountFn = peers
	trackerCountFn = trackers
}

// StartServer starts the Prometheus metrics HTTP server.
func StartServer(ctx context.Context, addr, path string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.Handler())
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		log.Info("Metrics server listening", "addr", addr, "path", path)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Metrics server error", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
}
