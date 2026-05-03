package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/L2jLiga/retracker/internal/config"
	"github.com/L2jLiga/retracker/internal/health"
	"github.com/L2jLiga/retracker/internal/metrics"
	"github.com/L2jLiga/retracker/internal/peers"
	"github.com/L2jLiga/retracker/internal/tracker"
	"github.com/L2jLiga/retracker/internal/upstream"
)

// Version info populated by linker flags
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

func main() {
	cfgPath := flag.String("config", "/etc/retracker/config.yaml", "path to config file")
	doHealthcheck := flag.Bool("healthcheck", false, "perform a healthcheck and exit")
	flag.Parse()

	// Docker HEALTHCHECK: probe /healthz and exit 0/1
	if *doHealthcheck {
		resp, err := http.Get("http://127.0.0.1:8080/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// ── Config ──────────────────────────────────────────────────────────────
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("Failed to load config", "err", err)
		os.Exit(1)
	}

	// ── Logger ───────────────────────────────────────────────────────────────
	log := newLogger(cfg.Log)
	log.Info("retracker starting")

	// ── Resolve interfaces ───────────────────────────────────────────────────
	ifaces, err := cfg.ResolveInterfaces()
	if err != nil {
		log.Error("Failed to resolve interfaces", "err", err)
		os.Exit(1)
	}
	log.Info("Resolved interfaces", "count", len(ifaces))

	// ── Context & signal handling ────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("Shutdown signal received")
		cancel()
	}()

	// ── Peer store ───────────────────────────────────────────────────────────
	store := peers.NewStore(
		cfg.Peers.MaxPerSwarm,
		cfg.Peers.PeerTimeout,
		cfg.Peers.BloomCapacity,
		cfg.Peers.BloomFPRate,
	)

	// ── Tracker list ─────────────────────────────────────────────────────────
	tList := tracker.NewTrackerList(
		cfg.Trackers.Static,
		cfg.Trackers.RemoteURL,
		cfg.Trackers.RefreshInterval,
		cfg.Trackers.FetchTimeout,
		log.WithGroup("trackerlist"),
	)
	tList.Start(ctx)

	// ── Upstream announcer ───────────────────────────────────────────────────
	ann := upstream.New(ifaces, tList, store, cfg.Peers.AnnounceInterval, log.WithGroup("upstream"))
	ann.Start(ctx)

	// ── HTTP tracker ─────────────────────────────────────────────────────────
	httpTracker := tracker.NewHTTPTracker(store, cfg.Peers.AnnounceInterval, ann, log.WithGroup("http"))
	httpSrv := tracker.NewHTTPServer(cfg.Listen.HTTP, httpTracker.Handler(), log.WithGroup("http"))
	go func() {
		if err := httpSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP tracker error", "err", err)
			cancel()
		}
	}()

	// ── UDP tracker ──────────────────────────────────────────────────────────
	udpTracker := tracker.NewUDPTracker(store, cfg.Peers.AnnounceInterval, ann, log.WithGroup("udp"))
	go func() {
		if err := udpTracker.ListenAndServe(cfg.Listen.UDP); err != nil {
			log.Error("UDP tracker error", "err", err)
			cancel()
		}
	}()

	// ── GC loop ───────────────────────────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(cfg.Peers.GCInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				evicted, removed := store.GC()
				metrics.GCEvictions.Add(float64(evicted))
				log.Debug("GC completed", "evicted_peers", evicted, "removed_swarms", removed)
			}
		}
	}()

	// ── Metrics ────────────────────────────────────────────────────────────────
	if cfg.Metrics.Enabled {
		metrics.RegisterCallbacks(
			func() float64 { return float64(store.SwarmCount()) },
			func() float64 { return float64(store.TotalPeers()) },
			func() float64 { return float64(tList.Count()) },
		)
		metrics.StartServer(ctx, cfg.Metrics.Listen, cfg.Metrics.Path, log.WithGroup("metrics"))
	}

	// ── Health server ─────────────────────────────────────────────────────────
	healthSrv := health.New(cfg.Health.Listen, log.WithGroup("health"))
	healthSrv.Start(ctx)
	healthSrv.SetReady(true)

	log.Info("retracker ready",
		"http", cfg.Listen.HTTP,
		"udp", cfg.Listen.UDP,
		"interfaces", len(ifaces),
	)

	// ── Wait for shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()
	healthSrv.SetReady(false)
	log.Info("Shutting down…")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Warn("HTTP server shutdown error", "err", err)
	}
	log.Info("Goodbye")
}

func newLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}
