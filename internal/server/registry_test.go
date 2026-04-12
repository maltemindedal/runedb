package server

import (
	"errors"
	"strings"
	"testing"
)

func TestRegistryCloseAllReturnsJoinedErrors(t *testing.T) {
	registry := NewRegistry()
	registry.Add(&stubConn{closeErr: errors.New("close one")})
	registry.Add(&stubConn{closeErr: errors.New("close two")})

	err := registry.CloseAll()
	if err == nil {
		t.Fatal("CloseAll() error = nil, want joined close errors")
	}

	message := err.Error()
	if !strings.Contains(message, "close one") || !strings.Contains(message, "close two") {
		t.Fatalf("CloseAll() error = %q, want both close errors joined", message)
	}
}
