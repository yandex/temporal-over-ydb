package cache

import (
	"fmt"

	"go.temporal.io/server/service/history/tasks"
)

type keyRange struct {
	inclusiveFrom tasks.Key
	inclusiveTo   tasks.Key
}

func newKeyRange(inclusiveFrom, inclusiveTo tasks.Key) keyRange {
	return keyRange{
		inclusiveFrom: inclusiveFrom,
		inclusiveTo:   inclusiveTo,
	}
}

func (r *keyRange) String() string {
	return fmt.Sprintf("[%s, %s]", formatKey(r.inclusiveFrom), formatKey(r.inclusiveTo))
}

func (r *keyRange) contains(other keyRange) bool {
	return r.inclusiveFrom.CompareTo(other.inclusiveFrom) <= 0 && r.inclusiveTo.CompareTo(other.inclusiveTo) >= 0
}

// compareTo returns
// -1 if the range is before k
// 0 if the range contains k
// 1 if the range is after k
func (r *keyRange) compareTo(k tasks.Key) int {
	if k.CompareTo(r.inclusiveFrom) == -1 {
		// . k . [from . . . to] . . .
		return 1
	}
	if k.CompareTo(r.inclusiveTo) == 1 {
		// . . . [from . . . to] . . . k
		return -1
	}
	return 0
}
