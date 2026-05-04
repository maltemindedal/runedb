package storage

func (v *ValueObject) hashLen() (int, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		return v.CompactHash.len(), nil
	}
	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	return len(v.Hash), nil
}

func (v *ValueObject) hashGet(field string) ([]byte, bool, error) {
	if v == nil {
		return nil, false, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, false, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, false, errInvalidValueObjectState
		}
		value, ok := v.CompactHash.get(field)
		return value, ok, nil
	}
	if v.Hash == nil {
		return nil, false, errInvalidValueObjectState
	}
	value, ok := v.Hash[field]
	return value, ok, nil
}

func (v *ValueObject) hashSet(pairs []HashFieldValue) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}

	added := int64(0)
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		for _, pair := range pairs {
			if v.CompactHash.set(pair.Field, pair.Value) {
				added++
			}
		}
		return added, nil
	}

	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	for _, pair := range pairs {
		if _, exists := v.Hash[pair.Field]; !exists {
			added++
		}
		v.Hash[pair.Field] = cloneBytes(pair.Value)
	}
	return added, nil
}

func (v *ValueObject) hashDel(fields []string) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return 0, ErrWrongType
	}

	removed := int64(0)
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return 0, errInvalidValueObjectState
		}
		for _, field := range fields {
			if v.CompactHash.del(field) {
				removed++
			}
		}
		return removed, nil
	}

	if v.Hash == nil {
		return 0, errInvalidValueObjectState
	}
	for _, field := range fields {
		if _, exists := v.Hash[field]; exists {
			delete(v.Hash, field)
			removed++
		}
	}
	return removed, nil
}

func (v *ValueObject) hashEntries() ([]HashFieldValue, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, errInvalidValueObjectState
		}
		return v.CompactHash.all(), nil
	}
	if v.Hash == nil {
		return nil, errInvalidValueObjectState
	}

	entries := make([]HashFieldValue, 0, len(v.Hash))
	for field, raw := range v.Hash {
		entries = append(entries, HashFieldValue{Field: field, Value: raw})
	}
	return entries, nil
}

func (v *ValueObject) cloneHashValue(expiresAt int64) (*ValueObject, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindHash {
		return nil, ErrWrongType
	}
	if v.HashEncoding == ValueEncodingCompact {
		if v.CompactHash == nil {
			return nil, errInvalidValueObjectState
		}
		return newCompactHashValue(cloneCompactHash(v.CompactHash), expiresAt), nil
	}
	if v.Hash == nil {
		return nil, errInvalidValueObjectState
	}

	fields := make(map[string][]byte, len(v.Hash))
	for field, raw := range v.Hash {
		fields[field] = cloneBytes(raw)
	}
	return newHashValue(fields, expiresAt), nil
}

func (v *ValueObject) zsetLen() (int, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return 0, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return 0, errInvalidValueObjectState
		}
		return v.CompactZSet.len(), nil
	}
	if v.ZSet == nil {
		return 0, errInvalidValueObjectState
	}
	return v.ZSet.len(), nil
}

func (v *ValueObject) zsetAdd(entries []ZSetEntry) (int64, error) {
	if v == nil {
		return 0, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return 0, ErrWrongType
	}

	added := int64(0)
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return 0, errInvalidValueObjectState
		}
		for _, entry := range entries {
			if v.CompactZSet.add(entry.Member, entry.Score) {
				added++
			}
		}
		return added, nil
	}

	if v.ZSet == nil {
		return 0, errInvalidValueObjectState
	}
	for _, entry := range entries {
		if v.ZSet.add(string(entry.Member), entry.Score) {
			added++
		}
	}
	return added, nil
}

func (v *ValueObject) zsetRangeByRank(start, stop int) ([]ZSetRangeEntry, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return nil, errInvalidValueObjectState
		}
		return v.CompactZSet.rangeByRank(start, stop), nil
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}
	return v.ZSet.rangeByRank(start, stop), nil
}

func (v *ValueObject) cloneZSetValue(expiresAt int64) (*ValueObject, error) {
	if v == nil {
		return nil, errInvalidValueObjectState
	}
	if v.Kind != ValueKindZSet {
		return nil, ErrWrongType
	}
	if v.ZSetEncoding == ValueEncodingCompact {
		if v.CompactZSet == nil {
			return nil, errInvalidValueObjectState
		}
		return newCompactZSetValue(cloneCompactZSet(v.CompactZSet), expiresAt), nil
	}
	if v.ZSet == nil {
		return nil, errInvalidValueObjectState
	}
	return newZSetValue(cloneSortedSet(v.ZSet), expiresAt), nil
}

func newHashValueForPairs(pairs []HashFieldValue, expiresAt int64) *ValueObject {
	if len(pairs) <= compactHashMaxEntries {
		return newCompactHashValue(newCompactHash(pairs), expiresAt)
	}

	fields := make(map[string][]byte, len(pairs))
	for _, pair := range pairs {
		fields[pair.Field] = cloneBytes(pair.Value)
	}
	return newHashValue(fields, expiresAt)
}

func newZSetValueForEntries(entries []ZSetEntry, expiresAt int64) *ValueObject {
	if len(entries) <= compactZSetMaxEntries {
		return newCompactZSetValue(newCompactZSet(entries), expiresAt)
	}

	set := newSortedSet()
	for _, entry := range entries {
		set.add(string(entry.Member), entry.Score)
	}
	return newZSetValue(set, expiresAt)
}
