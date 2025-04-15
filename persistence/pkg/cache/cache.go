package cache

import (
	"fmt"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	ImmediateCacheSuggestion struct {
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		BatchSize          int32
	}

	ImmediateTaskCache interface {
		Invalidate()
		Put(tasks Tasks) error
		List(inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (bool, SortedTasks, error)
		Remove(exclusiveMaxTaskKey tasks.Key) error
		Expand(inclusiveFromKey, inclusiveToKey tasks.Key, tasks Tasks) error
		SuggestedExpansion() *ImmediateCacheSuggestion
		Statistics() Statistics
	}

	immediateTaskCacheImpl struct {
		baseTaskCacheImpl
	}

	noopImmediateTaskCacheImpl struct {
	}
)

func NewNoopImmediateTaskCache() ImmediateTaskCache {
	return noopImmediateTaskCacheImpl{}
}

func (n noopImmediateTaskCacheImpl) Invalidate() {
}

func (n noopImmediateTaskCacheImpl) Put(tasks Tasks) error {
	return nil
}

func (n noopImmediateTaskCacheImpl) List(inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (bool, SortedTasks, error) {
	return false, nil, nil
}

func (n noopImmediateTaskCacheImpl) Remove(exclusiveMaxTaskKey tasks.Key) error {
	return nil
}

func (n noopImmediateTaskCacheImpl) Expand(inclusiveFromKey, inclusiveToKey tasks.Key, tasks Tasks) error {
	return nil
}

func (n noopImmediateTaskCacheImpl) SuggestedExpansion() *ImmediateCacheSuggestion {
	return nil
}

func (n noopImmediateTaskCacheImpl) Statistics() Statistics {
	return Statistics{}
}

func NewImmediateTaskCache(logger log.Logger, metricsHandler metrics.Handler, capacity int) ImmediateTaskCache {
	if capacity < 1 {
		panic(fmt.Sprintf("invalid capacity: %d", capacity))
	}
	return &immediateTaskCacheImpl{
		baseTaskCacheImpl: baseTaskCacheImpl{
			logger:         logger,
			metricsHandler: metricsHandler,
			capacity:       capacity,
		},
	}
}

func (c *immediateTaskCacheImpl) List(inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (bool, SortedTasks, error) {
	if inclusiveMinTaskID > exclusiveMaxTaskID {
		return false, nil, newInvalidRequestError(fmt.Sprintf(
			"invalid requested range: inclusive min (%d) > exclusive max (%d)",
			inclusiveMinTaskID, exclusiveMaxTaskID))
	} else if inclusiveMinTaskID == exclusiveMaxTaskID {
		// for some reason persistence gets this kind of requests: inclusive min == exclusive max, always empty
		c.metricsHandler.Counter(emptyHitCounter.Name()).Record(1)
		return true, nil, nil
	}

	requestedRange := newKeyRange(
		tasks.NewImmediateKey(inclusiveMinTaskID),
		tasks.NewImmediateKey(exclusiveMaxTaskID).Prev(),
	)
	return c.list(requestedRange, batchSize)
}

func (c *immediateTaskCacheImpl) SuggestedExpansion() *ImmediateCacheSuggestion {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.validRange == nil {
		return nil
	}

	capLeft := c.capacity - c.totalTasksLocked()
	batch := min(defaultExpansionBatch, capLeft)
	if batch < minExpansionBatch {
		return nil
	}

	var exclusiveMaxTaskID int64
	// probably not the best logic, just adapting to a scheduled cache mechanics
	if len(c.futureTasks) == 0 {
		exclusiveMaxTaskID = c.validRange.inclusiveTo.TaskID + 1000
	} else {
		exclusiveMaxTaskID = c.futureTasks[len(c.futureTasks)-1].Key.TaskID + 1
	}
	return &ImmediateCacheSuggestion{
		InclusiveMinTaskID: c.validRange.inclusiveTo.TaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		BatchSize:          int32(batch),
	}
}
