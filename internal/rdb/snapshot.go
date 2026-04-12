package rdb

import "bytes"

var emptySnapshot = func() []byte {
	payload := make([]byte, 0, len(fileHeader)+1+8)
	payload = append(payload, fileHeader...)
	payload = append(payload, opcodeEOF)
	payload = append(payload, make([]byte, 8)...)
	return payload
}()

// EmptySnapshot returns the canonical empty RDB payload used for FULLRESYNC.
func EmptySnapshot() []byte {
	return bytes.Clone(emptySnapshot)
}
