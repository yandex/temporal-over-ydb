package cache

import (
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

const defaultExpansionBatch = 300
const minExpansionBatch = 10
const cacheAheadPeriod = time.Minute * 30

type (
	ScheduledCacheSuggestion struct {
		InclusiveMinKey          tasks.Key
		ExclusiveMaxVisibilityTS time.Time
		BatchSize                int32
	}

	Statistics struct {
		Capacity          int
		TotalTasksCount   int
		PendingTasksCount int
		FutureTasksCount  int
		TasksCount        int
		ValidRange        *keyRange
	}

	ScheduledTaskCache interface {
		Invalidate()
		Put(tasks Tasks) error
		List(inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (bool, SortedTasks, error)
		Remove(exclusiveMaxTaskKey tasks.Key) error
		Expand(inclusiveFromKey, inclusiveToKey tasks.Key, tasks Tasks) error
		SuggestedExpansion() *ScheduledCacheSuggestion
		Statistics() Statistics
	}

	scheduledTaskCacheImpl struct {
		baseTaskCacheImpl
	}

	noopScheduledTaskCacheImpl struct {
	}
)

func (n noopScheduledTaskCacheImpl) Invalidate() {
}

func (n noopScheduledTaskCacheImpl) Put(tasks Tasks) error {
	return nil
}

func (n noopScheduledTaskCacheImpl) List(inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (bool, SortedTasks, error) {
	return false, nil, nil
}

func (n noopScheduledTaskCacheImpl) Remove(exclusiveMaxTaskKey tasks.Key) error {
	return nil
}

func (n noopScheduledTaskCacheImpl) Expand(inclusiveFromKey, inclusiveToKey tasks.Key, tasks Tasks) error {
	return nil
}

func (n noopScheduledTaskCacheImpl) SuggestedExpansion() *ScheduledCacheSuggestion {
	return nil
}

func (n noopScheduledTaskCacheImpl) Statistics() Statistics {
	return Statistics{}
}

func (s *Statistics) String() string {
	validRange := "nil"
	if s.ValidRange != nil {
		validRange = s.ValidRange.String()
	}
	return fmt.Sprintf("cap: %d, pending tasks: %d, future tasks: %d, tasks: %d, valid range: %s",
		s.Capacity, s.PendingTasksCount, s.FutureTasksCount, s.TasksCount, validRange)
}

func NewNoopScheduledTaskCache() ScheduledTaskCache {
	return &noopScheduledTaskCacheImpl{}
}

func NewScheduledTaskCache(logger log.Logger, metricsHandler metrics.Handler, capacity int) ScheduledTaskCache {
	if capacity < 1 {
		panic(fmt.Sprintf("invalid capacity: %d", capacity))
	}
	return &scheduledTaskCacheImpl{
		baseTaskCacheImpl: baseTaskCacheImpl{
			logger:         logger,
			metricsHandler: metricsHandler,
			capacity:       capacity,
		},
	}
}

func (c *scheduledTaskCacheImpl) List(inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (ok bool, ts SortedTasks, err error) {
	if inclusiveMinVisibilityTS.Compare(exclusiveMaxVisibilityTS) == 1 {
		return false, nil, newInvalidRequestError(fmt.Sprintf(
			"invalid requested range: inclusive min (%s) > exclusive max (%s)",
			inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS))
	} else if inclusiveMinVisibilityTS.Compare(exclusiveMaxVisibilityTS) == 0 {
		// for some reason persistence gets this kind of requests: inclusive min == exclusive max, always empty
		c.metricsHandler.Counter(emptyHitCounter.Name()).Record(1)
		return true, nil, nil
	}

	requestedRange := newKeyRange(
		tasks.NewKey(inclusiveMinVisibilityTS, inclusiveMinTaskID),
		tasks.NewKey(exclusiveMaxVisibilityTS, 0).Prev(),
	)
	return c.list(requestedRange, batchSize)
}

func (c *scheduledTaskCacheImpl) SuggestedExpansion() *ScheduledCacheSuggestion {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.validRange == nil {
		return &ScheduledCacheSuggestion{
			InclusiveMinKey: tasks.NewKey(time.Now(), 0),
			BatchSize:       defaultExpansionBatch,
		}
	}

	capLeft := c.capacity - c.totalTasksLocked()
	batch := min(defaultExpansionBatch, capLeft)
	if batch < minExpansionBatch {
		return nil
	}
	if time.Until(c.validRange.inclusiveTo.FireTime).Seconds() > cacheAheadPeriod.Seconds() {
		return nil
	}
	randomDuration := time.Minute * time.Duration(5+rand.Intn(15)) // between 5 and 20 minutes
	exclusiveMaxVisibilityTS := c.validRange.inclusiveTo.FireTime.Add(randomDuration)
	return &ScheduledCacheSuggestion{
		InclusiveMinKey:          c.validRange.inclusiveTo.Next(),
		ExclusiveMaxVisibilityTS: exclusiveMaxVisibilityTS,
		BatchSize:                int32(batch),
	}
}
