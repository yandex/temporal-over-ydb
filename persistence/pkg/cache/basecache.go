package cache

import (
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	baseTaskCacheImpl struct {
		logger         log.Logger
		metricsHandler metrics.Handler
		capacity       int

		mu sync.Mutex

		validRange   *keyRange
		tasks        SortedTasks
		pendingTasks Tasks
		futureTasks  Tasks
	}
)

func (c *baseTaskCacheImpl) totalTasksLocked() int {
	return len(c.pendingTasks) + len(c.futureTasks) + len(c.tasks)
}

func (c *baseTaskCacheImpl) invalidateLocked() {
	c.validRange = nil
	c.tasks = nil
	c.pendingTasks = nil
	c.futureTasks = nil
}

func (c *baseTaskCacheImpl) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.invalidateLocked()
}

func (c *baseTaskCacheImpl) statisticsLocked() Statistics {
	return Statistics{
		Capacity:          c.capacity,
		TotalTasksCount:   c.totalTasksLocked(),
		PendingTasksCount: len(c.pendingTasks),
		FutureTasksCount:  len(c.futureTasks),
		TasksCount:        len(c.tasks),
		ValidRange:        c.validRange,
	}
}

func (c *baseTaskCacheImpl) Statistics() Statistics {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.statisticsLocked()
}

// putLocked puts the tasks into the cache.
// It assumes that all tasks are within or after the valid range, never before.
func (c *baseTaskCacheImpl) putLocked(tasks Tasks) error {
	var pendingCount int64 = 0
	var futureCount int64 = 0
	for _, task := range tasks {
		pos := c.validRange.compareTo(task.Key)
		if pos == -1 {
			c.futureTasks = append(c.futureTasks, task)
			futureCount++
		} else {
			c.pendingTasks = append(c.pendingTasks, task)
			pendingCount++
		}
	}
	c.metricsHandler.Counter(tasksPutCounter.Name()).Record(pendingCount)
	c.metricsHandler.Counter(futureTasksPutCounter.Name()).Record(futureCount)

	if c.totalTasksLocked() > c.capacity {
		c.compactLocked()

		if len(c.futureTasks) > len(c.tasks) {
			err := newTooManyFutureTasks(c.statisticsLocked())
			c.invalidateLocked()
			return err
		}
	}

	return nil
}

func (c *baseTaskCacheImpl) Expand(inclusiveFromKey, inclusiveToKey tasks.Key, newTasks Tasks) (err error) {
	defer func() {
		if err != nil {
			c.logger.Error(err.Error())
			c.metricsHandler.Counter(writeErrorCounter.Name()).Record(1)
		}
	}()

	expansionRange := newKeyRange(inclusiveFromKey, inclusiveToKey)

	if expansionRange.inclusiveFrom.CompareTo(expansionRange.inclusiveTo) == 1 {
		return newInvalidRequestError(fmt.Sprintf("invalid expansion range: inclusive from (%s) > exclusive to (%s)",
			formatKey(expansionRange.inclusiveFrom), formatKey(expansionRange.inclusiveTo)))
	}

	tasksRange := newTasks.getRange()
	if !expansionRange.contains(tasksRange) {
		return newInvalidRequestError(fmt.Sprintf("invalid expansion range: %s does not contain tasks range %s",
			expansionRange.String(), tasksRange.String()))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// First we update the valid range, as putLocked separates current tasks from future tasks
	if c.validRange == nil {
		c.validRange = &expansionRange
	} else {
		validRangePlusOne := newKeyRange(c.validRange.inclusiveFrom, c.validRange.inclusiveTo.Next())
		if validRangePlusOne.compareTo(expansionRange.inclusiveFrom) != 0 {
			return newInvalidRequestError(
				fmt.Sprintf("invalid expansion range: inclusive from key %s is not in valid range and does not touch it from the right: %s",
					formatKey(expansionRange.inclusiveFrom), c.validRange.String()))
		}
		c.validRange.inclusiveTo = tasks.MaxKey(c.validRange.inclusiveTo, expansionRange.inclusiveTo)
	}

	// Take the portion of future tasks that is covered by inclusiveToKey
	currTasks, futureTasks := c.futureTasks.sort().splitBy(expansionRange.inclusiveTo)
	c.futureTasks = Tasks(futureTasks)

	// Put the tasks
	return c.putLocked(append(newTasks, currTasks...))
}

func (c *baseTaskCacheImpl) Put(tasks Tasks) (err error) {
	if len(tasks) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.validRange == nil {
		return newInvalidCacheError()
	}

	defer func() {
		if err != nil {
			c.logger.Error(err.Error())
			c.metricsHandler.Counter(writeErrorCounter.Name()).Record(1)
		}
	}()

	for _, task := range tasks {
		if c.validRange.compareTo(task.Key) == 1 {
			return newInvalidRequestError(fmt.Sprintf("task %s is below the valid range: %s",
				formatKey(task.Key), c.validRange))
		}
	}

	return c.putLocked(tasks)
}

func (c *baseTaskCacheImpl) compactLocked() {
	mergedTasks := c.tasks.merge(c.pendingTasks.sort())
	capLeft := c.capacity - len(mergedTasks) - len(c.futureTasks)
	if capLeft < 0 {
		s := min(-capLeft, len(mergedTasks))
		c.validRange.inclusiveFrom = mergedTasks[s-1].Key.Next()
		mergedTasks = mergedTasks[s:]
	}
	c.tasks = mergedTasks
	c.pendingTasks = nil
}

func (c *baseTaskCacheImpl) list(requestedRange keyRange, batchSize int32) (ok bool, ts SortedTasks, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.validRange == nil {
		return false, nil, newInvalidCacheError()
	}

	defer func() {
		if err != nil {
			c.logger.Error(err.Error())
			c.metricsHandler.Counter(readErrorCounter.Name()).Record(1)
		}
	}()

	c.compactLocked()

	if c.validRange.contains(requestedRange) {
		res := c.tasks.query(requestedRange, int(batchSize))
		c.metricsHandler.Counter(hitCounter.Name()).Record(1)
		c.metricsHandler.Counter(tasksReadCounter.Name()).Record(int64(len(res)))
		return true, res, nil
	} else {
		c.metricsHandler.Counter(missCounter.Name()).Record(1)
		return false, nil, nil
	}
}

func (c *baseTaskCacheImpl) Remove(exclusiveMaxTaskKey tasks.Key) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.validRange == nil {
		return newInvalidCacheError()
	}

	defer func() {
		if err != nil {
			c.logger.Error(err.Error())
			c.metricsHandler.Counter(writeErrorCounter.Name()).Record(1)
		}
	}()

	c.compactLocked()

	inclusiveMaxTaskKey := exclusiveMaxTaskKey.Prev()
	pos := c.validRange.compareTo(inclusiveMaxTaskKey)
	if pos == -1 {
		return newInvalidRequestError(
			fmt.Sprintf("exclusive max key %s is above the valid range: %s",
				formatKey(exclusiveMaxTaskKey), c.validRange.String()))
	} else if pos == 1 {
		return nil
	} else {
		_, c.tasks = c.tasks.splitBy(inclusiveMaxTaskKey)
	}
	return nil
}

func formatKey(key tasks.Key) string {
	if key.FireTime.IsZero() {
		return fmt.Sprintf("(%d)", key.TaskID)
	} else {
		return fmt.Sprintf("(%s, %d)", key.FireTime.Format(time.StampMilli), key.TaskID)
	}
}
