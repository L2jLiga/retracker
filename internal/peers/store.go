// Package peers implements the in-memory peer store.
// Keys are [20]byte (info_hash, peer_id) — never string — as required by the
// BitTorrent specs.
package peers

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

// InfoHash is a 20-byte SHA1 info_hash (BEP 3 / BEP 15).
type InfoHash [20]byte

// PeerID is a 20-byte peer_id (BEP 3 / BEP 15).
type PeerID [20]byte

// Peer represents a single peer in a swarm.
type Peer struct {
	ID       PeerID
	IP       net.IP
	Port     uint16
	LastSeen time.Time
	// Uploaded/Downloaded/Left — stored for re-announce
	Uploaded   int64
	Downloaded int64
	Left       int64
	Event      string
}

// Swarm holds all peers for a single info_hash.
type Swarm struct {
	mu      sync.RWMutex
	peers   map[PeerID]*Peer
	seeders int
	leechers int
}

func newSwarm() *Swarm {
	return &Swarm{peers: make(map[PeerID]*Peer)}
}

func (s *Swarm) Upsert(p *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.peers[p.ID]
	if exists {
		if old.Left == 0 {
			s.seeders--
		} else {
			s.leechers--
		}
	}
	s.peers[p.ID] = p
	if p.Left == 0 {
		s.seeders++
	} else {
		s.leechers++
	}
}

func (s *Swarm) Remove(id PeerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.peers[id]; ok {
		if p.Left == 0 {
			s.seeders--
		} else {
			s.leechers--
		}
		delete(s.peers, id)
	}
}

// Peers returns a snapshot of up to max peers, excluding the requesting peer.
func (s *Swarm) Peers(exclude PeerID, max int) []*Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Peer, 0, max)
	for id, p := range s.peers {
		if id == exclude {
			continue
		}
		out = append(out, p)
		if len(out) >= max {
			break
		}
	}
	return out
}

// Stats returns (seeders, leechers, completed) — completed is not tracked here.
func (s *Swarm) Stats() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seeders, s.leechers
}

func (s *Swarm) evict(timeout time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-timeout)
	evicted := 0
	for id, p := range s.peers {
		if p.LastSeen.Before(cutoff) {
			if p.Left == 0 {
				s.seeders--
			} else {
				s.leechers--
			}
			delete(s.peers, id)
			evicted++
		}
	}
	return evicted
}

// Store is the thread-safe peer store.
type Store struct {
	mu          sync.RWMutex
	swarms      map[InfoHash]*Swarm
	maxPerSwarm int
	peerTimeout time.Duration
	bloom       *bloom.BloomFilter
	bloomMu     sync.Mutex
}

// NewStore creates a peer store with the given limits.
func NewStore(maxPerSwarm int, peerTimeout time.Duration, bloomCap uint, bloomFP float64) *Store {
	return &Store{
		swarms:      make(map[InfoHash]*Swarm),
		maxPerSwarm: maxPerSwarm,
		peerTimeout: peerTimeout,
		bloom:       bloom.NewWithEstimates(bloomCap, bloomFP),
	}
}

// Announce registers/updates a peer and returns peers for the swarm.
func (s *Store) Announce(ih InfoHash, p *Peer, numWant int) []*Peer {
	s.mu.Lock()
	sw, ok := s.swarms[ih]
	if !ok {
		sw = newSwarm()
		s.swarms[ih] = sw
	}
	s.mu.Unlock()

	if p.Event == "stopped" {
		sw.Remove(p.ID)
	} else {
		sw.Upsert(p)
	}

	max := numWant
	if max <= 0 || max > s.maxPerSwarm {
		max = s.maxPerSwarm
	}
	return sw.Peers(p.ID, max)
}

// Scrape returns (seeders, leechers, completed) for a list of info_hashes.
func (s *Store) Scrape(hashes []InfoHash) map[InfoHash][3]int32 {
	out := make(map[InfoHash][3]int32, len(hashes))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ih := range hashes {
		if sw, ok := s.swarms[ih]; ok {
			sd, lc := sw.Stats()
			out[ih] = [3]int32{int32(sd), int32(lc), 0}
		}
	}
	return out
}

// Swarms returns a snapshot of all known info_hashes and their swarms
// (used by the upstream re-announcer).
func (s *Store) Swarms() map[InfoHash]*Swarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[InfoHash]*Swarm, len(s.swarms))
	for k, v := range s.swarms {
		out[k] = v
	}
	return out
}

// SwarmCount returns the number of tracked swarms.
func (s *Store) SwarmCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.swarms)
}

// TotalPeers returns total number of tracked peers across all swarms.
func (s *Store) TotalPeers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for _, sw := range s.swarms {
		sw.mu.RLock()
		total += len(sw.peers)
		sw.mu.RUnlock()
	}
	return total
}

// GC removes expired peers and empty swarms.
func (s *Store) GC() (evictedPeers, removedSwarms int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ih, sw := range s.swarms {
		n := sw.evict(s.peerTimeout)
		evictedPeers += n
		sw.mu.RLock()
		empty := len(sw.peers) == 0
		sw.mu.RUnlock()
		if empty {
			delete(s.swarms, ih)
			removedSwarms++
		}
	}
	return
}

// BloomCheck returns true if this (info_hash, peer_id) combo was recently seen.
// Also adds it to the filter if not present.
func (s *Store) BloomCheck(ih InfoHash, pid PeerID) bool {
	key := make([]byte, 40)
	copy(key[:20], ih[:])
	copy(key[20:], pid[:])
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()
	if s.bloom.Test(key) {
		return true
	}
	s.bloom.Add(key)
	return false
}

// BloomReset clears the bloom filter (call periodically to avoid saturation).
func (s *Store) BloomReset() {
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()
	s.bloom.ClearAll()
}

// ── Compact peer encoding (BEP 3 / BEP 23) ──────────────────────────────────

// CompactIPv4 encodes peers as 6-byte (IP4:port) compact format (BEP 23).
func CompactIPv4(peers []*Peer) []byte {
	buf := make([]byte, 0, len(peers)*6)
	for _, p := range peers {
		ip4 := p.IP.To4()
		if ip4 == nil {
			continue
		}
		buf = append(buf, ip4...)
		buf = binary.BigEndian.AppendUint16(buf, p.Port)
	}
	return buf
}

// CompactIPv6 encodes peers as 18-byte (IP6:port) compact format (BEP 7).
func CompactIPv6(peers []*Peer) []byte {
	buf := make([]byte, 0, len(peers)*18)
	for _, p := range peers {
		ip6 := p.IP.To16()
		if ip6 == nil || p.IP.To4() != nil {
			continue
		}
		buf = append(buf, ip6...)
		buf = binary.BigEndian.AppendUint16(buf, p.Port)
	}
	return buf
}
