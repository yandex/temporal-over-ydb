package persistence

import (
	"math"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	NumHistoryShards      = 1024
	minYDBDateTime        = getMinYDBDateTime()
	maxYDBDateTime        = time.Unix(math.MaxInt32, 0).UTC()
	currentExecutionRunID = types.UTF8Value("")
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

func ToShardIDColumnValue(shardID int32) uint32 {
	// Uniformly spread shard across (0, math.MaxUint32) interval
	step := math.MaxUint32/NumHistoryShards - 1
	return uint32(shardID * int32(step))
}
