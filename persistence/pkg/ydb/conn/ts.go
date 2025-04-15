package conn

import (
	"math"
	"time"
)

var (
	minYDBDateTime = getMinYDBDateTime()
	maxYDBDateTime = time.Unix(math.MaxInt32, 0).UTC()
)

// ToYDBDateTime converts to time to YDB datetime
func ToYDBDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minYDBDateTime
	}
	if t.After(maxYDBDateTime) {
		return maxYDBDateTime
	}
	return t.UTC()
}

// FromYDBDateTime converts YDB datetime and returns go time
func FromYDBDateTime(t time.Time) time.Time {
	if t.Equal(minYDBDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinYDBDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
