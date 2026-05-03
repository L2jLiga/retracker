package tracker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
	"math/rand"
	"net"
	"time"

	"github.com/l2jliga/retracker/internal/metrics"
	"github.com/l2jliga/retracker/internal/peers"
)

// BEP 15 action codes
const (
	actionConnect  = 0
	actionAnnounce = 1
	actionScrape   = 2
	actionError    = 3
)

const (
	udpMagicConnID = 0x41727101980 // BEP 15 magic value
	maxUDPPacket   = 65507
	// Connection IDs are valid for 2 minutes (BEP 15 §1)
	connIDTTL = 2 * time.Minute
)

// UDPTracker handles BEP 15 UDP tracker protocol.
type UDPTracker struct {
	store    *peers.Store
	interval time.Duration
	upstream UpstreamNotifier
	log      *slog.Logger
	secret   []byte // for connection ID HMAC
}

func NewUDPTracker(store *peers.Store, interval time.Duration, up UpstreamNotifier, log *slog.Logger) *UDPTracker {
	secret := make([]byte, 32)
	rand.Read(secret) //nolint:gosec
	return &UDPTracker{
		store:    store,
		interval: interval,
		upstream: up,
		log:      log,
		secret:   secret,
	}
}

// ListenAndServe starts the UDP tracker on the given address.
func (u *UDPTracker) ListenAndServe(addr string) error {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer pc.Close()
	u.log.Info("UDP tracker listening", "addr", addr)
	buf := make([]byte, maxUDPPacket)
	for {
		n, src, err := pc.ReadFrom(buf)
		if err != nil {
			u.log.Warn("UDP read error", "err", err)
			continue
		}
		go u.handle(pc, src, buf[:n])
	}
}

func (u *UDPTracker) handle(pc net.PacketConn, src net.Addr, pkt []byte) {
	if len(pkt) < 16 {
		return
	}
	connID := int64(binary.BigEndian.Uint64(pkt[0:8]))
	action := binary.BigEndian.Uint32(pkt[8:12])
	txID := binary.BigEndian.Uint32(pkt[12:16])

	metrics.UDPRequests.WithLabelValues(actionName(action)).Inc()

	switch action {
	case actionConnect:
		u.handleConnect(pc, src, txID, connID)
	case actionAnnounce:
		u.handleAnnounce(pc, src, txID, connID, pkt)
	case actionScrape:
		u.handleScrape(pc, src, txID, connID, pkt)
	default:
		u.sendError(pc, src, txID, "unknown action")
	}
}

// handleConnect validates the magic and issues a connection ID (BEP 15 §1).
func (u *UDPTracker) handleConnect(pc net.PacketConn, src net.Addr, txID uint32, connID int64) {
	if connID != udpMagicConnID {
		u.sendError(pc, src, txID, "invalid connection id")
		return
	}
	newConnID := u.makeConnID(src, time.Now())
	resp := make([]byte, 16)
	binary.BigEndian.PutUint32(resp[0:4], actionConnect)
	binary.BigEndian.PutUint32(resp[4:8], txID)
	binary.BigEndian.PutUint64(resp[8:16], uint64(newConnID))
	_, _ = pc.WriteTo(resp, src)
}

// handleAnnounce implements BEP 15 §2 announce.
// Packet layout (98 bytes minimum):
//   8  connection_id
//   4  action (1)
//   4  transaction_id
//  20  info_hash
//  20  peer_id
//   8  downloaded
//   8  left
//   8  uploaded
//   4  event
//   4  IP address (0 = use sender)
//   4  key
//   4  num_want (-1 = default)
//   2  port
func (u *UDPTracker) handleAnnounce(pc net.PacketConn, src net.Addr, txID, _ uint32, pkt []byte) {
	if len(pkt) < 98 {
		u.sendError(pc, src, txID, "packet too short")
		return
	}

	connID := int64(binary.BigEndian.Uint64(pkt[0:8]))
	if !u.validateConnID(connID, src) {
		u.sendError(pc, src, txID, "invalid connection id")
		return
	}

	var ih peers.InfoHash
	copy(ih[:], pkt[16:36])
	var pid peers.PeerID
	copy(pid[:], pkt[36:56])

	downloaded := int64(binary.BigEndian.Uint64(pkt[56:64]))
	left := int64(binary.BigEndian.Uint64(pkt[64:72]))
	uploaded := int64(binary.BigEndian.Uint64(pkt[72:80]))
	event := udpEvent(binary.BigEndian.Uint32(pkt[80:84]))
	rawIP := binary.BigEndian.Uint32(pkt[84:88])
	//key := binary.BigEndian.Uint32(pkt[88:92]) // not used
	numWant := int32(binary.BigEndian.Uint32(pkt[92:96]))
	port := binary.BigEndian.Uint16(pkt[96:98])

	// Determine peer IP
	var ip net.IP
	if rawIP != 0 {
		ip = make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, rawIP)
	} else {
		ip = udpSrcIP(src)
	}

	if numWant <= 0 || numWant > 74 {
		numWant = 74 // BEP 15 §2 default
	}

	peer := &peers.Peer{
		ID:         pid,
		IP:         ip,
		Port:       port,
		Uploaded:   uploaded,
		Downloaded: downloaded,
		Left:       left,
		Event:      event,
		LastSeen:   time.Now(),
	}

	peerList := u.store.Announce(ih, peer, int(numWant))
	metrics.PeersAnnounced.Inc()

	if u.upstream != nil && event != "stopped" {
		u.upstream.Notify(ih, peer)
	}

	seeders, leechers := func() (int32, int32) {
		stats := u.store.Scrape([]peers.InfoHash{ih})
		if s, ok := stats[ih]; ok {
			return s[0], s[1]
		}
		return 0, 0
	}()

	// Build compact response (only IPv4 for UDP tracker per BEP 15)
	ipv4Peers := make([]*peers.Peer, 0, len(peerList))
	for _, p := range peerList {
		if p.IP.To4() != nil {
			ipv4Peers = append(ipv4Peers, p)
		}
	}
	compact := peers.CompactIPv4(ipv4Peers)

	resp := make([]byte, 20+len(compact))
	binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
	binary.BigEndian.PutUint32(resp[4:8], txID)
	binary.BigEndian.PutUint32(resp[8:12], uint32(u.interval.Seconds()))
	binary.BigEndian.PutUint32(resp[12:16], uint32(leechers))
	binary.BigEndian.PutUint32(resp[16:20], uint32(seeders))
	copy(resp[20:], compact)
	_, _ = pc.WriteTo(resp, src)
}

// handleScrape implements BEP 15 §3 scrape.
// Packet layout: 16 header + N*20 info_hashes
func (u *UDPTracker) handleScrape(pc net.PacketConn, src net.Addr, txID, _ uint32, pkt []byte) {
	connID := int64(binary.BigEndian.Uint64(pkt[0:8]))
	if !u.validateConnID(connID, src) {
		u.sendError(pc, src, txID, "invalid connection id")
		return
	}

	body := pkt[16:]
	n := len(body) / 20
	hashes := make([]peers.InfoHash, n)
	for i := 0; i < n; i++ {
		copy(hashes[i][:], body[i*20:(i+1)*20])
	}

	stats := u.store.Scrape(hashes)

	resp := make([]byte, 8+n*12)
	binary.BigEndian.PutUint32(resp[0:4], actionScrape)
	binary.BigEndian.PutUint32(resp[4:8], txID)
	for i, ih := range hashes {
		s := stats[ih]
		off := 8 + i*12
		binary.BigEndian.PutUint32(resp[off:off+4], uint32(s[0]))   // seeders
		binary.BigEndian.PutUint32(resp[off+4:off+8], uint32(s[2])) // completed
		binary.BigEndian.PutUint32(resp[off+8:off+12], uint32(s[1])) // leechers
	}
	_, _ = pc.WriteTo(resp, src)
}

func (u *UDPTracker) sendError(pc net.PacketConn, src net.Addr, txID uint32, msg string) {
	b := make([]byte, 8+len(msg))
	binary.BigEndian.PutUint32(b[0:4], actionError)
	binary.BigEndian.PutUint32(b[4:8], txID)
	copy(b[8:], msg)
	_, _ = pc.WriteTo(b, src)
}

// makeConnID generates a time-bucketed HMAC connection ID (BEP 15 security).
func (u *UDPTracker) makeConnID(src net.Addr, t time.Time) int64 {
	bucket := t.Truncate(connIDTTL).Unix()
	h := hmac.New(sha256.New, u.secret)
	h.Write([]byte(src.String()))
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(bucket))
	h.Write(b)
	sum := h.Sum(nil)
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// validateConnID checks the current and previous time bucket.
func (u *UDPTracker) validateConnID(connID int64, src net.Addr) bool {
	now := time.Now()
	if connID == u.makeConnID(src, now) {
		return true
	}
	// Allow previous bucket too
	if connID == u.makeConnID(src, now.Add(-connIDTTL)) {
		return true
	}
	return false
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func udpSrcIP(src net.Addr) net.IP {
	switch a := src.(type) {
	case *net.UDPAddr:
		return a.IP
	}
	host, _, err := net.SplitHostPort(src.String())
	if err != nil {
		return nil
	}
	return net.ParseIP(host)
}

func udpEvent(e uint32) string {
	switch e {
	case 1:
		return "completed"
	case 2:
		return "started"
	case 3:
		return "stopped"
	default:
		return ""
	}
}

func actionName(a uint32) string {
	switch a {
	case actionConnect:
		return "connect"
	case actionAnnounce:
		return "announce"
	case actionScrape:
		return "scrape"
	default:
		return "unknown"
	}
}
