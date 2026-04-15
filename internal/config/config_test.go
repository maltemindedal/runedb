package config

import "testing"

func TestDefaultIncludesShutdownSnapshotPath(t *testing.T) {
	cfg := Default()

	if cfg.DumpPath != "dump.rdb" {
		t.Fatalf("DumpPath = %q, want %q", cfg.DumpPath, "dump.rdb")
	}
	if cfg.MasterAuth != "" {
		t.Fatalf("MasterAuth = %q, want empty string", cfg.MasterAuth)
	}
}
