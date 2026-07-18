package storage

import (
	"slices"
	"strconv"
)

// intSetMaxEntries bounds how many members the compact sorted-slice IntSet
// encoding holds before the set upgrades to the general hashtable. Past this
// size, per-member add/remove on the sorted slice (O(n)) and repeated merges
// (O(n) each, so O(n^2) to build incrementally) cost more than the map. Mirrors
// Redis' set-max-intset-entries default.
const intSetMaxEntries = 512

// IntSet stores integer-only set members in sorted int64 order.
type IntSet struct {
	values []int64
}

func newIntSet(members [][]byte) (*IntSet, bool) {
	values, ok := parseIntSetMembers(members)
	if !ok {
		return nil, false
	}
	slices.Sort(values)
	values = compactUniqueInts(values)
	return &IntSet{values: values}, true
}

func cloneIntSet(src *IntSet) *IntSet {
	if src == nil {
		return nil
	}
	return &IntSet{values: append([]int64(nil), src.values...)}
}

func (s *IntSet) len() int {
	if s == nil {
		return 0
	}
	return len(s.values)
}

func (s *IntSet) addMany(members [][]byte) (int64, bool) {
	values, ok := parseIntSetMembers(members)
	if !ok {
		return 0, false
	}
	if len(values) == 0 {
		return 0, true
	}
	slices.Sort(values)
	values = compactUniqueInts(values)
	if len(s.values) == 0 {
		s.values = values
		return int64(len(values)), true
	}

	merged := make([]int64, 0, len(s.values)+len(values))
	added := int64(0)
	i, j := 0, 0
	for i < len(s.values) && j < len(values) {
		left, right := s.values[i], values[j]
		switch {
		case left < right:
			merged = append(merged, left)
			i++
		case left > right:
			merged = append(merged, right)
			j++
			added++
		default:
			merged = append(merged, left)
			i++
			j++
		}
	}
	merged = append(merged, s.values[i:]...)
	if j < len(values) {
		added += int64(len(values) - j)
		merged = append(merged, values[j:]...)
	}
	s.values = merged
	return added, true
}

func (s *IntSet) contains(member []byte) bool {
	if s == nil {
		return false
	}
	value, ok := parseIntSetMember(member)
	if !ok {
		return false
	}
	_, exists := slices.BinarySearch(s.values, value)
	return exists
}

func (s *IntSet) remove(member []byte) bool {
	if s == nil {
		return false
	}
	value, ok := parseIntSetMember(member)
	if !ok {
		return false
	}
	index, exists := slices.BinarySearch(s.values, value)
	if !exists {
		return false
	}
	s.values = slices.Delete(s.values, index, index+1)
	return true
}

func (s *IntSet) members() [][]byte {
	if s == nil {
		return nil
	}
	members := make([][]byte, 0, len(s.values))
	for _, value := range s.values {
		members = append(members, strconv.AppendInt(make([]byte, 0, 20), value, 10))
	}
	return members
}

func (s *IntSet) generalSet() map[string]struct{} {
	members := make(map[string]struct{}, s.len())
	if s == nil {
		return members
	}
	for _, value := range s.values {
		members[strconv.FormatInt(value, 10)] = struct{}{}
	}
	return members
}

func parseIntSetMembers(members [][]byte) ([]int64, bool) {
	values := make([]int64, 0, len(members))
	for _, member := range members {
		value, ok := parseIntSetMember(member)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func parseIntSetMember(member []byte) (int64, bool) {
	text := string(member)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(value, 10) != text {
		return 0, false
	}
	return value, true
}

func compactUniqueInts(values []int64) []int64 {
	if len(values) < 2 {
		return values
	}
	write := 1
	for _, value := range values[1:] {
		if value == values[write-1] {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}
