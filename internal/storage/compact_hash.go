package storage

const compactHashMaxEntries = 8

type compactHashEntry struct {
	fieldStart int
	fieldLen   int
	valueStart int
	valueLen   int
}

// CompactHash stores small hash values as contiguous field/value bytes plus
// compact entry metadata. It is selected internally without changing the
// client-visible hash value kind.
type CompactHash struct {
	entries []compactHashEntry
	arena   []byte
}

func newCompactHash(pairs []HashFieldValue) *CompactHash {
	h := &CompactHash{entries: make([]compactHashEntry, 0, len(pairs))}
	for _, pair := range pairs {
		h.set(pair.Field, pair.Value)
	}
	return h
}

func cloneCompactHash(src *CompactHash) *CompactHash {
	if src == nil {
		return nil
	}

	return &CompactHash{
		entries: append([]compactHashEntry(nil), src.entries...),
		arena:   cloneBytes(src.arena),
	}
}

func (h *CompactHash) len() int {
	if h == nil {
		return 0
	}
	return len(h.entries)
}

func (h *CompactHash) get(field string) ([]byte, bool) {
	if h == nil {
		return nil, false
	}
	for _, entry := range h.entries {
		if h.fieldEqual(entry, field) {
			return h.value(entry), true
		}
	}
	return nil, false
}

func (h *CompactHash) set(field string, value []byte) bool {
	for i, entry := range h.entries {
		if h.fieldEqual(entry, field) {
			if len(value) <= entry.valueLen {
				copy(h.arena[entry.valueStart:entry.valueStart+entry.valueLen], value)
				h.entries[i].valueLen = len(value)
			} else {
				h.rebuild(i, HashFieldValue{Field: field, Value: value})
			}
			return false
		}
	}

	fieldStart := len(h.arena)
	h.arena = append(h.arena, field...)
	valueStart := len(h.arena)
	h.arena = append(h.arena, value...)
	h.entries = append(h.entries, compactHashEntry{
		fieldStart: fieldStart,
		fieldLen:   len(field),
		valueStart: valueStart,
		valueLen:   len(value),
	})
	return true
}

func (h *CompactHash) del(field string) bool {
	if h == nil {
		return false
	}
	for i, entry := range h.entries {
		if !h.fieldEqual(entry, field) {
			continue
		}
		h.rebuild(i)
		return true
	}
	return false
}

func (h *CompactHash) all() []HashFieldValue {
	if h == nil {
		return nil
	}
	entries := make([]HashFieldValue, 0, len(h.entries))
	for _, entry := range h.entries {
		entries = append(entries, HashFieldValue{Field: h.field(entry), Value: h.value(entry)})
	}
	return entries
}

func (h *CompactHash) rebuild(skip int, replacements ...HashFieldValue) {
	pairs := make([]HashFieldValue, 0, len(h.entries)+len(replacements))
	pairs = append(pairs, replacements...)
	for i, entry := range h.entries {
		if i == skip {
			continue
		}
		pairs = append(pairs, HashFieldValue{Field: h.field(entry), Value: h.value(entry)})
	}
	rebuilt := newCompactHash(pairs)
	h.entries = rebuilt.entries
	h.arena = rebuilt.arena
}

func (h *CompactHash) field(entry compactHashEntry) string {
	return string(h.arena[entry.fieldStart : entry.fieldStart+entry.fieldLen])
}

func (h *CompactHash) fieldEqual(entry compactHashEntry, field string) bool {
	if entry.fieldLen != len(field) {
		return false
	}
	for i, b := range h.arena[entry.fieldStart : entry.fieldStart+entry.fieldLen] {
		if b != field[i] {
			return false
		}
	}
	return true
}

func (h *CompactHash) value(entry compactHashEntry) []byte {
	return h.arena[entry.valueStart : entry.valueStart+entry.valueLen]
}
