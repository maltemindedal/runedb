package config

import (
	"flag"
	"fmt"
	"time"
)

// Config contains the runtime settings for the RuneDB server.
type Config struct {
	Host               string
	Port               int
	LogLevel           string
	EvictionInterval   time.Duration
	EvictionSampleSize int
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
