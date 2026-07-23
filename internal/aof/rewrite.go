package aof

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// GenerateRewrite emits the shortest practical RESP command stream for the supplied snapshot.
func GenerateRewrite(entries []storage.SnapshotEntry, writer io.Writer) (RewriteStats, error) {
	sorted := make([]storage.SnapshotEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	now := time.Now().UnixMilli()
	stats := RewriteStats{}
	for _, entry := range sorted {
		frames, err := rewriteFramesForEntry(entry, now)
		if err != nil {
			return stats, err
		}
		if len(frames) == 0 {
			continue
		}
		stats.Keys++
		stats.Commands += len(frames)
		for _, frame := range frames {
			if err := protocol.WriteValue(writer, frame); err != nil {
				return stats, fmt.Errorf("aof: write rewrite frame for key %q: %w", entry.Key, err)
			}
		}
	}

	return stats, nil
}

func rewriteFramesForEntry(entry storage.SnapshotEntry, now int64) ([]protocol.Value, error) {
	switch entry.Kind {
	case storage.ValueKindString:
		if entry.ExpiresAt > 0 && entry.ExpiresAt <= now {
			return nil, nil
		}
		args := []protocol.Value{bulkString(entry.Key), bulkBytes(entry.String)}
		if entry.ExpiresAt > 0 {
			ttl := entry.ExpiresAt - now
			if ttl <= 0 {
				return nil, nil
			}
			args = append(args, bulkString("PX"), bulkString(strconv.FormatInt(ttl, 10)))
		}
		return []protocol.Value{rewriteCommand("SET", args...)}, nil
	case storage.ValueKindList:
		if len(entry.List) == 0 {
			return nil, nil
		}
		args := make([]protocol.Value, 0, len(entry.List)+1)
		args = append(args, bulkString(entry.Key))
		for _, item := range entry.List {
			args = append(args, bulkBytes(item))
		}
		return []protocol.Value{rewriteCommand("RPUSH", args...)}, nil
	case storage.ValueKindHash:
		if len(entry.Hash) == 0 {
			return nil, nil
		}
		sorted := make([]storage.HashFieldValue, len(entry.Hash))
		copy(sorted, entry.Hash)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Field < sorted[j].Field
		})
		args := make([]protocol.Value, 0, len(sorted)*2+1)
		args = append(args, bulkString(entry.Key))
		for _, field := range sorted {
			args = append(args, bulkString(field.Field), bulkBytes(field.Value))
		}
		return []protocol.Value{rewriteCommand("HSET", args...)}, nil
	case storage.ValueKindSet:
		if len(entry.Set) == 0 {
			return nil, nil
		}
		members := make([]string, 0, len(entry.Set))
		for _, member := range entry.Set {
			members = append(members, string(member))
		}
		sort.Strings(members)
		args := make([]protocol.Value, 0, len(members)+1)
		args = append(args, bulkString(entry.Key))
		for _, member := range members {
			args = append(args, bulkString(member))
		}
		return []protocol.Value{rewriteCommand("SADD", args...)}, nil
	case storage.ValueKindZSet:
		if len(entry.ZSet) == 0 {
			return nil, nil
		}
		args := make([]protocol.Value, 0, len(entry.ZSet)*2+1)
		args = append(args, bulkString(entry.Key))
		for _, zsetEntry := range entry.ZSet {
			args = append(args, bulkString(strconv.FormatFloat(zsetEntry.Score, 'g', -1, 64)), bulkString(zsetEntry.Member))
		}
		return []protocol.Value{rewriteCommand("ZADD", args...)}, nil
	case storage.ValueKindStream:
		frames := make([]protocol.Value, 0, len(entry.Stream))
		for _, streamEntry := range entry.Stream {
			args := make([]protocol.Value, 0, len(streamEntry.Values)+2)
			args = append(args, bulkString(entry.Key), bulkString(streamEntry.ID))
			for _, value := range streamEntry.Values {
				args = append(args, bulkBytes(value))
			}
			frames = append(frames, rewriteCommand("XADD", args...))
		}
		return frames, nil
	default:
		return nil, fmt.Errorf("aof: unsupported rewrite value kind %q for key %q", entry.Kind, entry.Key)
	}
}

func rewriteCommand(name string, args ...protocol.Value) protocol.Array {
	elements := make([]protocol.Value, 0, len(args)+1)
	elements = append(elements, protocol.TextBulkString{Value: name})
	elements = append(elements, args...)
	return protocol.Array{Elements: elements}
}

func bulkString(value string) protocol.BulkString {
	return protocol.BulkString{Data: []byte(value)}
}

func bulkBytes(value []byte) protocol.BulkString {
	return protocol.BulkString{Data: value}
}
