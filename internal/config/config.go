package config

import (
	"flag"
	"fmt"
	"net"
	"os"
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
	DumpPath           string
	AOFPath            string
	AppendFsync        string
	ReplicaOf          string
	MasterAuth         string
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
		DumpPath:           "dump.rdb",
		AOFPath:            "",
		AppendFsync:        "everysec",
		ReplicaOf:          "",
		MasterAuth:         "",
	}
}

// ParseFlags parses command-line flags into a Config value.
func ParseFlags() Config {
	cfg, err := parseFlags(flag.CommandLine, os.Args[1:])
	if err == nil {
		return cfg
	}

	_, _ = fmt.Fprintf(os.Stderr, "failed to parse flags: %v\n", err)
	os.Exit(2)
	return Config{}
}

func parseFlags(fs *flag.FlagSet, args []string) (Config, error) {
	cfg := Default()

	fs.StringVar(&cfg.Host, "host", cfg.Host, "host interface to bind the TCP listener to")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level: debug, info, warn, error")
	fs.DurationVar(&cfg.EvictionInterval, "eviction-interval", cfg.EvictionInterval, "interval for active TTL eviction")
	fs.IntVar(&cfg.EvictionSampleSize, "eviction-sample-size", cfg.EvictionSampleSize, "number of keys to sample on each eviction pass")
	fs.StringVar(&cfg.RDBPath, "rdb", cfg.RDBPath, "optional path to an RDB file to load before accepting TCP connections")
	fs.StringVar(&cfg.DumpPath, "dump", cfg.DumpPath, "path to write an RDB snapshot during graceful shutdown")
	fs.StringVar(&cfg.AOFPath, "aof", cfg.AOFPath, "optional path to an append-only file used for durable command logging")
	fs.StringVar(&cfg.ReplicaOf, "replicaof", cfg.ReplicaOf, "optional master address in host:port form for replica mode")
	fs.StringVar(&cfg.MasterAuth, "masterauth", cfg.MasterAuth, "optional password used by replica mode to AUTH against a protected master")
	fs.StringVar(&cfg.RequirePass, "requirepass", cfg.RequirePass, "optional password required for AUTH-protected client commands")
	fs.Func("appendfsync", "appendfsync policy: always, everysec, no", func(value string) error {
		normalized, err := normalizeAppendFsync(value)
		if err != nil {
			return err
		}

		cfg.AppendFsync = normalized
		return nil
	})

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	return cfg, nil
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

func normalizeAppendFsync(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "always", "everysec", "no":
		return normalized, nil
	default:
		return "", fmt.Errorf("invalid appendfsync policy %q: expected always, everysec, or no", value)
	}
}
