package config

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"
)

// Config contains the runtime settings for the RuneDB server.
type Config struct {
	Host               string
	Port               int
	LogLevel           string
	EvictionInterval   time.Duration
	EvictionSampleSize int
	RDBPath            string
	ReplicaOf          string
	RequirePass        string
}

// Default returns the default runtime configuration for local development.
func Default() Config {
	return Config{
		Host:               "",
		Port:               6379,
		LogLevel:           "info",
		EvictionInterval:   100 * time.Millisecond,
		EvictionSampleSize: 20,
		RDBPath:            "",
		ReplicaOf:          "",
	}
}

// ParseFlags parses command-line flags into a Config value.
func ParseFlags() Config {
	cfg := Default()

	flag.StringVar(&cfg.Host, "host", cfg.Host, "host interface to bind the TCP listener to")
	flag.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug, info, warn, error")
	flag.DurationVar(&cfg.EvictionInterval, "eviction-interval", cfg.EvictionInterval, "interval for active TTL eviction")
	flag.IntVar(&cfg.EvictionSampleSize, "eviction-sample-size", cfg.EvictionSampleSize, "number of keys to sample on each eviction pass")
	flag.StringVar(&cfg.RDBPath, "rdb", cfg.RDBPath, "optional path to an RDB file to load before accepting TCP connections")
	flag.StringVar(&cfg.ReplicaOf, "replicaof", cfg.ReplicaOf, "optional master address in host:port form for replica mode")
	flag.StringVar(&cfg.RequirePass, "requirepass", cfg.RequirePass, "optional password requirement placeholder for future AUTH support")
	flag.Parse()

	return cfg
}

// Address formats the listen address used by net.Listen.
func (c Config) Address() string {
	if c.Host == "" {
		return fmt.Sprintf(":%d", c.Port)
	}

	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// IsReplica reports whether the server should connect to an upstream master.
func (c Config) IsReplica() bool {
	return strings.TrimSpace(c.ReplicaOf) != ""
}

// ReplicaAddress validates and normalizes the configured upstream master address.
func (c Config) ReplicaAddress() (string, error) {
	address := strings.TrimSpace(c.ReplicaOf)
	if address == "" {
		return "", nil
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid replicaof address %q: %w", address, err)
	}
	if host == "" || port == "" {
		return "", fmt.Errorf("invalid replicaof address %q: expected host:port", address)
	}

	return net.JoinHostPort(host, port), nil
}
