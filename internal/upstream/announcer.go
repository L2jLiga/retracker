// Package upstream handles re-announcing torrents to external trackers via
// each configured network interface.
package upstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/l2jliga/retracker/internal/config"
	"github.com/l2jliga/retracker/internal/metrics"
	"github.com/l2jliga/retracker/internal/peers"
)

// TrackerProvider is implemented by tracker.TrackerList.
type TrackerProvider interface {
	All() []string
}

// Announcer re-announces torrents to upstream trackers over multiple interfaces.
type Announcer struct {
	ifaces   []config.ResolvedInterface
	trackers TrackerProvider
	store    *peers.Store
	interval time.Duration
	log      *slog.Logger
	// notify channel: inbound (ih, peer) pairs from local tracker handlers
	notifyCh chan notifyMsg
}

type notifyMsg struct {
	ih   peers.InfoHash
	peer *peers.Peer
}

func New(
	ifaces []config.ResolvedInterface,
	trackers TrackerProvider,
	store *peers.Store,
	interval time.Duration,
	log *slog.Logger,
) *Announcer {
	return &Announcer{
		ifaces:   ifaces,
		trackers: trackers,
		store:    store,
		interval: interval,
		log:      log,
		notifyCh: make(chan notifyMsg, 1024),
	}
}

// Notify is called by the local tracker handler when a new announce arrives.
func (a *Announcer) Notify(ih peers.InfoHash, p *peers.Peer) {
	select {
	case a.notifyCh <- notifyMsg{ih, p}:
	default:
		// Drop if buffer full
	}
}

// Start begins processing notifications and periodic re-announces.
func (a *Announcer) Start(ctx context.Context) {
	go a.processNotifications(ctx)
	go a.periodicReannounce(ctx)
}

// processNotifications handles inbound announces and fans them out.
func (a *Announcer) processNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-a.notifyCh:
			for _, iface := range a.ifaces {
				iface := iface
				go a.reannounceAll(ctx, msg.ih, msg.peer, iface)
			}
		}
	}
}

// periodicReannounce re-announces all known swarms periodically.
func (a *Announcer) periodicReannounce(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			swarms := a.store.Swarms()
			a.log.Info("Periodic re-announce", "swarms", len(swarms))
			for ih, sw := range swarms {
				ih := ih
				sw := sw
				peerList := sw.Peers(peers.PeerID{}, 1)
				if len(peerList) == 0 {
					continue
				}
				p := peerList[0]
				for _, iface := range a.ifaces {
					iface := iface
					go a.reannounceAll(ctx, ih, p, iface)
				}
			}
		}
	}
}

// reannounceAll sends an announce for the given info_hash + peer to every
// known upstream tracker, using the given interface.
func (a *Announcer) reannounceAll(ctx context.Context, ih peers.InfoHash, p *peers.Peer, iface config.ResolvedInterface) {
	trackerURLs := a.trackers.All()
	for _, tURL := range trackerURLs {
		tURL := tURL
		go func() {
			var err error
			if strings.HasPrefix(tURL, "udp://") {
				err = a.announceUDP(ctx, ih, p, iface, tURL)
			} else {
				err = a.announceHTTP(ctx, ih, p, iface, tURL)
			}
			if err != nil {
				a.log.Debug("Upstream announce failed",
					"tracker", tURL, "interface", iface.BindAddr, "err", err)
				metrics.UpstreamErrors.WithLabelValues(tURL).Inc()
			} else {
				metrics.UpstreamAnnounces.WithLabelValues(tURL).Inc()
			}
		}()
	}
}

// ── HTTP upstream announce ────────────────────────────────────────────────────

func (a *Announcer) announceHTTP(ctx context.Context, ih peers.InfoHash, p *peers.Peer, iface config.ResolvedInterface, trackerURL string) error {
	// Choose public IP based on tracker URL scheme and available IPs
	pubIP := selectPublicIP(iface, p.IP)
	if pubIP == nil {
		return fmt.Errorf("no public IP for interface %s", iface.BindAddr)
	}

	// Build the announce URL
	q := url.Values{}
	q.Set("info_hash", string(ih[:]))
	q.Set("peer_id", string(p.ID[:]))
	q.Set("port", strconv.Itoa(int(p.Port)))
	q.Set("uploaded", strconv.FormatInt(p.Uploaded, 10))
	q.Set("downloaded", strconv.FormatInt(p.Downloaded, 10))
	q.Set("left", strconv.FormatInt(p.Left, 10))
	q.Set("compact", "1")
	q.Set("numwant", "0") // We don't need peers from upstream
	q.Set("ip", pubIP.String())
	if p.Event != "" {
		q.Set("event", p.Event)
	}

	announceURL := trackerURL + "/announce?" + q.Encode()

	// Bind HTTP transport to the specific local IP
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP(iface.BindAddr)},
			Timeout:   10 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, announceURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "retracker/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// ── UDP upstream announce (BEP 15) ───────────────────────────────────────────

func (a *Announcer) announceUDP(ctx context.Context, ih peers.InfoHash, p *peers.Peer, iface config.ResolvedInterface, trackerURL string) error {
	pubIP := selectPublicIP(iface, p.IP)
	if pubIP == nil {
		return fmt.Errorf("no public IP for interface %s", iface.BindAddr)
	}

	u, err := url.Parse(trackerURL)
	if err != nil {
		return fmt.Errorf("parse tracker URL: %w", err)
	}

	raddr, err := net.ResolveUDPAddr("udp", u.Host)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", u.Host, err)
	}

	laddr := &net.UDPAddr{IP: net.ParseIP(iface.BindAddr), Port: 0}
	conn, err := net.DialUDP("udp", laddr, raddr)
	if err != nil {
		return fmt.Errorf("dial UDP: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second)) //nolint:errcheck

	// Step 1: Connect
	connID, err := udpConnect(conn)
	if err != nil {
		return fmt.Errorf("UDP connect: %w", err)
	}

	// Step 2: Announce
	txID := rand.Uint32() //nolint:gosec
	pkt := make([]byte, 98)
	binary.BigEndian.PutUint64(pkt[0:8], uint64(connID))
	binary.BigEndian.PutUint32(pkt[8:12], 1) // action = announce
	binary.BigEndian.PutUint32(pkt[12:16], txID)
	copy(pkt[16:36], ih[:])
	copy(pkt[36:56], p.ID[:])
	binary.BigEndian.PutUint64(pkt[56:64], uint64(p.Downloaded))
	binary.BigEndian.PutUint64(pkt[64:72], uint64(p.Left))
	binary.BigEndian.PutUint64(pkt[72:80], uint64(p.Uploaded))
	binary.BigEndian.PutUint32(pkt[80:84], udpEventCode(p.Event))
	// IP: put public IP if IPv4
	if ip4 := pubIP.To4(); ip4 != nil {
		binary.BigEndian.PutUint32(pkt[84:88], binary.BigEndian.Uint32(ip4))
	}
	binary.BigEndian.PutUint32(pkt[88:92], rand.Uint32()) //nolint:gosec key
	binary.BigEndian.PutUint32(pkt[92:96], 0xFFFFFFFF)   // num_want = -1 → 0 effectively
	binary.BigEndian.PutUint16(pkt[96:98], p.Port)

	if _, err := conn.Write(pkt); err != nil {
		return err
	}

	resp := make([]byte, 512)
	n, err := conn.Read(resp)
	if err != nil {
		return err
	}
	if n < 8 {
		return fmt.Errorf("short response")
	}
	action := binary.BigEndian.Uint32(resp[0:4])
	if action == 3 {
		return fmt.Errorf("tracker error: %s", string(resp[8:n]))
	}
	if action != 1 {
		return fmt.Errorf("unexpected action %d", action)
	}
	return nil
}

func udpConnect(conn *net.UDPConn) (int64, error) {
	txID := rand.Uint32() //nolint:gosec
	pkt := make([]byte, 16)
	binary.BigEndian.PutUint64(pkt[0:8], 0x41727101980) // magic
	binary.BigEndian.PutUint32(pkt[8:12], 0)            // action = connect
	binary.BigEndian.PutUint32(pkt[12:16], txID)

	if _, err := conn.Write(pkt); err != nil {
		return 0, err
	}

	resp := make([]byte, 16)
	n, err := conn.Read(resp)
	if err != nil {
		return 0, err
	}
	if n < 16 {
		return 0, fmt.Errorf("connect response too short")
	}
	if binary.BigEndian.Uint32(resp[4:8]) != txID {
		return 0, fmt.Errorf("transaction id mismatch")
	}
	return int64(binary.BigEndian.Uint64(resp[8:16])), nil
}

func udpEventCode(event string) uint32 {
	switch event {
	case "completed":
		return 1
	case "started":
		return 2
	case "stopped":
		return 3
	default:
		return 0
	}
}

// selectPublicIP returns the public IP to use for outgoing announces.
// It prefers IPv4 or IPv6 based on the original peer IP family.
func selectPublicIP(iface config.ResolvedInterface, peerIP net.IP) net.IP {
	if peerIP.To4() != nil && iface.PubIPv4 != nil {
		return iface.PubIPv4
	}
	if iface.PubIPv6 != nil {
		return iface.PubIPv6
	}
	if iface.PubIPv4 != nil {
		return iface.PubIPv4
	}
	return nil
}

// ── Interface-bound HTTP client pool ─────────────────────────────────────────

// ClientPool caches HTTP clients per interface bind address.
type ClientPool struct {
	mu      sync.RWMutex
	clients map[string]*http.Client
}

func NewClientPool() *ClientPool {
	return &ClientPool{clients: make(map[string]*http.Client)}
}

func (cp *ClientPool) Get(bindAddr string) *http.Client {
	cp.mu.RLock()
	c, ok := cp.clients[bindAddr]
	cp.mu.RUnlock()
	if ok {
		return c
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	c = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				LocalAddr: &net.TCPAddr{IP: net.ParseIP(bindAddr)},
				Timeout:   10 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConnsPerHost: 4,
		},
		Timeout: 15 * time.Second,
	}
	cp.clients[bindAddr] = c
	return c
}
