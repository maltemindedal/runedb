package config

import (
	"flag"
	"io"
	"testing"
)

func TestDefaultIncludesShutdownSnapshotPath(t *testing.T) {
	cfg := Default()

	if cfg.DumpPath != "dump.rdb" {
		t.Fatalf("DumpPath = %q, want %q", cfg.DumpPath, "dump.rdb")
	}
	if cfg.MasterAuth != "" {
		t.Fatalf("MasterAuth = %q, want empty string", cfg.MasterAuth)
	}
	if cfg.AppendFsync != "everysec" {
		t.Fatalf("AppendFsync = %q, want %q", cfg.AppendFsync, "everysec")
	}
	if cfg.MaxMemory != 0 {
		t.Fatalf("MaxMemory = %d, want 0", cfg.MaxMemory)
	}
}

func TestParseFlags(t *testing.T) {
	t.Run("parses AOF flags", func(t *testing.T) {
		fs := flag.NewFlagSet("runedb-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		cfg, err := parseFlags(fs, []string{"--aof", "appendonly.aof", "--appendfsync", "always", "--dump", "snapshot.rdb", "--maxmemory", "2048"})
		if err != nil {
			t.Fatalf("parseFlags() error = %v", err)
		}
		if cfg.AOFPath != "appendonly.aof" {
			t.Fatalf("AOFPath = %q, want %q", cfg.AOFPath, "appendonly.aof")
		}
		if cfg.AppendFsync != "always" {
			t.Fatalf("AppendFsync = %q, want %q", cfg.AppendFsync, "always")
		}
		if cfg.DumpPath != "snapshot.rdb" {
			t.Fatalf("DumpPath = %q, want %q", cfg.DumpPath, "snapshot.rdb")
		}
		if cfg.MaxMemory != 2048 {
			t.Fatalf("MaxMemory = %d, want 2048", cfg.MaxMemory)
		}
	})

	t.Run("rejects invalid appendfsync policy", func(t *testing.T) {
		fs := flag.NewFlagSet("runedb-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		if _, err := parseFlags(fs, []string{"--appendfsync", "sometimes"}); err == nil {
			t.Fatal("parseFlags() error = nil, want invalid appendfsync failure")
		}
	})

	t.Run("rejects negative maxmemory", func(t *testing.T) {
		fs := flag.NewFlagSet("runedb-test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)

		if _, err := parseFlags(fs, []string{"--maxmemory", "-1"}); err == nil {
			t.Fatal("parseFlags() error = nil, want negative maxmemory failure")
		}
	})
}
