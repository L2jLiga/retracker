# retracker

A lightweight multi-interface BitTorrent re-tracker written in Go.

## Overview

`retracker` sits between qBittorrent and public trackers. qBittorrent announces
to it on `localhost`, and retracker re-announces the torrent to all configured
upstream trackers — once per configured network interface — substituting the
correct public IP for each path. This gives you multi-homed peer discovery
without any qBittorrent patches.

```
qBittorrent → retracker(:6969) ──[eth0/ISP1]──→ tracker1, tracker2, …
                                └──[eth1/ISP2]──→ tracker1, tracker2, …
```

## Protocol compliance

| BEP | Description | Status |
|-----|-------------|--------|
| BEP 3 | HTTP tracker protocol | ✅ |
| BEP 15 | UDP tracker protocol | ✅ (connection ID HMAC security) |
| BEP 23 | Compact peer lists (IPv4 + IPv6) | ✅ |
| BEP 7 | IPv6 tracker extension | ✅ |

## Quick start

```bash
# 1. Copy and edit the config
cp config.example.yaml config.yaml
$EDITOR config.yaml

# 2. Run with Docker
docker run -d \
  -p 6969:6969/tcp -p 6969:6969/udp \
  -p 9090:9090 -p 8080:8080 \
  -v $(pwd)/config.yaml:/etc/retracker/config.yaml:ro \
  --network host \   # or use macvlan per docker-compose.yml
  ghcr.io/L2jLiga/retracker:latest

# 3. Point qBittorrent at:
#    http://<retracker-host>:6969/announce
#    udp://<retracker-host>:6969/announce
```

## Configuration

See [`config.example.yaml`](config.example.yaml) for all options.

Key sections:

```yaml
interfaces:
  - name: eth0
    public_ipv4: "203.0.113.10"
  - name: eth1
    public_ipv4: "203.0.113.20"

trackers:
  remote_url: "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt"
  refresh_interval: 60m
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `:6969/announce` | HTTP tracker (BEP 3) |
| `:6969/scrape` | HTTP scrape (BEP 48) |
| `:6969` UDP | UDP tracker (BEP 15) |
| `:8080/healthz` | Health / Docker HEALTHCHECK |
| `:8080/livez` | Liveness probe |
| `:8080/readyz` | Readiness probe |
| `:9090/metrics` | Prometheus metrics |

## Build

```bash
go build -o retracker ./cmd/retracker

# Or with Docker:
docker build -t retracker .
```

## Docker images

Multi-arch images are published to:
- `ghcr.io/L2jLiga/retracker`
- `docker.io/L2jLiga/retracker`

Supported architectures: `amd64`, `arm64`, `arm/v7`, `386`, `ppc64le`, `s390x`, `riscv64`

## Design notes

- `info_hash` and `peer_id` stored as `[20]byte` — never as strings
- Bloom filter (configurable capacity / FP rate) deduplicates rapid re-announces
- Peer GC runs on a configurable interval; swarms with no peers are removed
- UDP security via HMAC connection IDs (2-minute rolling window, BEP 15 §1)
- Per-interface `net.Dialer` binds outbound TCP/UDP to the correct local IP
- `retracker` is designed for use on trusted internal networks only

## License

MIT
