package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration structure.
type Config struct {
	Listen    ListenConfig    `yaml:"listen"`
	Peers     PeersConfig     `yaml:"peers"`
	Trackers  TrackersConfig  `yaml:"trackers"`
	Interfaces []InterfaceCfg `yaml:"interfaces"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Health    HealthConfig    `yaml:"health"`
	Log       LogConfig       `yaml:"log"`
}

type ListenConfig struct {
	HTTP string `yaml:"http"` // e.g. ":6969"
	UDP  string `yaml:"udp"`  // e.g. ":6969"
}

type PeersConfig struct {
	MaxPerSwarm    int           `yaml:"max_per_swarm"`
	AnnounceInterval time.Duration `yaml:"announce_interval"`
	GCInterval     time.Duration `yaml:"gc_interval"`
	PeerTimeout    time.Duration `yaml:"peer_timeout"`
	BloomCapacity  uint          `yaml:"bloom_capacity"`
	BloomFPRate    float64       `yaml:"bloom_fp_rate"`
}

type TrackersConfig struct {
	// Static list of tracker URLs
	Static []string `yaml:"static"`
	// Remote URL returning newline-separated tracker URLs
	RemoteURL      string        `yaml:"remote_url"`
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	FetchTimeout   time.Duration `yaml:"fetch_timeout"`
}

// InterfaceCfg maps a local network interface (or IP) to the public IP
// that the outside world sees for outbound connections on that interface.
type InterfaceCfg struct {
	// Name of the interface, e.g. "eth0". Mutually exclusive with BindIP.
	Name string `yaml:"name"`
	// BindIP is used instead of Name when you want to bind to a specific
	// local IP rather than an entire interface.
	BindIP string `yaml:"bind_ip"`
	// PublicIP is the public-facing IP associated with this interface/IP.
	// Used when rewriting peer IP in announce requests forwarded upstream.
	PublicIPv4 string `yaml:"public_ipv4"`
	PublicIPv6 string `yaml:"public_ipv6"`
}

// ResolvedInterface is an InterfaceCfg with parsed net.IP fields.
type ResolvedInterface struct {
	InterfaceCfg
	BindAddr   string // resolved bind address (host only)
	PubIPv4    net.IP
	PubIPv6    net.IP
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"` // e.g. ":9090"
	Path    string `yaml:"path"`   // default "/metrics"
}

type HealthConfig struct {
	Listen string `yaml:"listen"` // e.g. ":8080"
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // text, json
}

// Defaults fills in sane defaults for zero-value fields.
func (c *Config) Defaults() {
	if c.Listen.HTTP == "" {
		c.Listen.HTTP = ":6969"
	}
	if c.Listen.UDP == "" {
		c.Listen.UDP = ":6969"
	}
	if c.Peers.MaxPerSwarm == 0 {
		c.Peers.MaxPerSwarm = 50
	}
	if c.Peers.AnnounceInterval == 0 {
		c.Peers.AnnounceInterval = 30 * time.Minute
	}
	if c.Peers.GCInterval == 0 {
		c.Peers.GCInterval = 5 * time.Minute
	}
	if c.Peers.PeerTimeout == 0 {
		c.Peers.PeerTimeout = 90 * time.Minute
	}
	if c.Peers.BloomCapacity == 0 {
		c.Peers.BloomCapacity = 100000
	}
	if c.Peers.BloomFPRate == 0 {
		c.Peers.BloomFPRate = 0.01
	}
	if c.Trackers.RefreshInterval == 0 {
		c.Trackers.RefreshInterval = 60 * time.Minute
	}
	if c.Trackers.FetchTimeout == 0 {
		c.Trackers.FetchTimeout = 30 * time.Second
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}
	if c.Metrics.Listen == "" {
		c.Metrics.Listen = ":9090"
	}
	if c.Health.Listen == "" {
		c.Health.Listen = ":8080"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
}

// Validate checks that required fields are present and valid.
func (c *Config) Validate() error {
	for i, iface := range c.Interfaces {
		if iface.Name == "" && iface.BindIP == "" {
			return fmt.Errorf("interface[%d]: must set either name or bind_ip", i)
		}
		if iface.PublicIPv4 == "" && iface.PublicIPv6 == "" {
			return fmt.Errorf("interface[%d]: must set at least one of public_ipv4 or public_ipv6", i)
		}
		if iface.PublicIPv4 != "" && net.ParseIP(iface.PublicIPv4) == nil {
			return fmt.Errorf("interface[%d]: invalid public_ipv4 %q", i, iface.PublicIPv4)
		}
		if iface.PublicIPv6 != "" && net.ParseIP(iface.PublicIPv6) == nil {
			return fmt.Errorf("interface[%d]: invalid public_ipv6 %q", i, iface.PublicIPv6)
		}
		if iface.BindIP != "" && net.ParseIP(iface.BindIP) == nil {
			return fmt.Errorf("interface[%d]: invalid bind_ip %q", i, iface.BindIP)
		}
	}
	return nil
}

// ResolveInterfaces resolves interface configs into ResolvedInterface structs.
func (c *Config) ResolveInterfaces() ([]ResolvedInterface, error) {
	out := make([]ResolvedInterface, 0, len(c.Interfaces))
	for _, iface := range c.Interfaces {
		ri := ResolvedInterface{InterfaceCfg: iface}
		if iface.PublicIPv4 != "" {
			ri.PubIPv4 = net.ParseIP(iface.PublicIPv4).To4()
		}
		if iface.PublicIPv6 != "" {
			ri.PubIPv6 = net.ParseIP(iface.PublicIPv6).To16()
		}
		if iface.BindIP != "" {
			ri.BindAddr = iface.BindIP
		} else {
			// Resolve bind IP from interface name
			ni, err := net.InterfaceByName(iface.Name)
			if err != nil {
				return nil, fmt.Errorf("interface %q: %w", iface.Name, err)
			}
			addrs, err := ni.Addrs()
			if err != nil {
				return nil, fmt.Errorf("interface %q addrs: %w", iface.Name, err)
			}
			for _, a := range addrs {
				var ip net.IP
				switch v := a.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip == nil || ip.IsLoopback() {
					continue
				}
				if ip.To4() != nil {
					ri.BindAddr = ip.String()
					break
				}
			}
			if ri.BindAddr == "" {
				return nil, fmt.Errorf("interface %q: no usable IPv4 address found", iface.Name)
			}
		}
		out = append(out, ri)
	}
	return out, nil
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}
