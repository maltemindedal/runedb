package storage

import "math/bits"

// MaxBitmapOffset is the largest supported Redis-compatible bitmap bit offset.
const MaxBitmapOffset = (1 << 32) - 1

// GetBit returns the bit stored at offset for a string value.
func (s *Store) GetBit(key string, offset int64) (int64, bool, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, false, err
	}

	return readKey(s, key, func(value *ValueObject) (int64, error) {
		data, err := value.StringValue()
		if err != nil {
			return 0, err
		}
		return bitAt(data, offset), nil
	})
}

// SetBit sets the bit stored at offset for a string value and returns the
// previous bit. When maxmemory is configured it first frees space and reports
// the keys evicted to make room; otherwise it evicts nothing.
func (s *Store) SetBit(key string, offset int64, bit int64) (int64, []string, error) {
	if err := validateBitmapArguments(offset, bit); err != nil {
		return 0, nil, err
	}

	return writeKey(s, key, func(w keyWrite) (int64, []string, error) {
		var (
			data      []byte
			expiresAt int64
		)
		if w.current != nil {
			current, err := w.current.StringValue()
			if err != nil {
				return 0, nil, err
			}
			data = current
			expiresAt = w.current.ExpiresAt
		}

		previous := bitAt(data, offset)
		byteIndex := int(offset / 8)
		mask := byte(1 << uint(7-(offset%8)))
		if byteIndex < len(data) && !w.accounting {
			// The payload already reaches the offset, so the bit can be flipped
			// where it lies. While accounting the write still goes through commit
			// below, which sizes it against the stored value and therefore needs
			// that value intact.
			setBitInByte(&data[byteIndex], mask, bit)
			w.current.touch(w.now)
			return previous, nil, nil
		}

		evicted, err := w.commitString(max(byteIndex+1, len(data)), expiresAt, func(payload []byte) {
			copy(payload, data)
			setBitInByte(&payload[byteIndex], mask, bit)
		})
		if err != nil {
			return 0, nil, err
		}
		return previous, evicted, nil
	})
}

func validateBitmapArguments(offset int64, bit int64) error {
	if err := validateBitmapOffset(offset); err != nil {
		return err
	}
	if bit != 0 && bit != 1 {
		return ErrValueNotInteger
	}
	return nil
}

func validateBitmapOffset(offset int64) error {
	if offset < 0 || offset > MaxBitmapOffset {
		return ErrValueNotInteger
	}
	return nil
}

func setBitInByte(dst *byte, mask byte, bit int64) {
	if bit == 1 {
		*dst |= mask
	} else {
		*dst &^= mask
	}
}

// BitCount counts set bits in a string value, optionally over an inclusive byte range.
func (s *Store) BitCount(key string, start *int64, end *int64) (int64, bool, error) {
	return readKey(s, key, func(value *ValueObject) (int64, error) {
		data, err := value.StringValue()
		if err != nil {
			return 0, err
		}
		return countBits(data, start, end), nil
	})
}

func bitAt(data []byte, offset int64) int64 {
	byteIndex := int(offset / 8)
	if byteIndex >= len(data) {
		return 0
	}

	mask := byte(1 << uint(7-(offset%8)))
	if data[byteIndex]&mask == 0 {
		return 0
	}
	return 1
}

func countBits(data []byte, start *int64, end *int64) int64 {
	if len(data) == 0 {
		return 0
	}

	from, to, ok := normalizeBitmapRange(len(data), start, end)
	if !ok {
		return 0
	}

	count := int64(0)
	for _, b := range data[from : to+1] {
		count += int64(bits.OnesCount8(b))
	}
	return count
}

func normalizeBitmapRange(length int, start *int64, end *int64) (int, int, bool) {
	if start == nil || end == nil {
		return 0, length - 1, true
	}

	from := *start
	to := *end
	if from < 0 {
		from = int64(length) + from
	}
	if to < 0 {
		to = int64(length) + to
	}
	if from < 0 {
		from = 0
	}
	if to < 0 || from >= int64(length) || from > to {
		return 0, 0, false
	}
	if to >= int64(length) {
		to = int64(length) - 1
	}

	return int(from), int(to), true
}
