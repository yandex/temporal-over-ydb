package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
)

type (
	TaskCacheFactory interface {
		GetOrCreateImmediateTaskCache(shardID int32, category tasks.Category) ImmediateTaskCache
		GetOrCreateScheduledTaskCache(shardID int32, category tasks.Category) ScheduledTaskCache
		InvalidateAllCaches(shardID int32)
		Stop()
	}

	taskCacheFactoryImpl struct {
		logger                log.Logger
		metricsHandler        metrics.Handler
		cacheCapacity         int
		transferTaskCaches    map[int32]ImmediateTaskCache
		visibilityTaskCaches  map[int32]ImmediateTaskCache
		replicationTaskCaches map[int32]ImmediateTaskCache
		timerTaskCaches       map[int32]ScheduledTaskCache
		m                     sync.Mutex

		metricsReporter cacheMetricsReporter
	}
	noopTaskCacheFactoryImpl struct {
	}
)

func NewNoopTaskCacheFactory() TaskCacheFactory {
	return noopTaskCacheFactoryImpl{}
}

func (n noopTaskCacheFactoryImpl) GetOrCreateImmediateTaskCache(shardID int32, category tasks.Category) ImmediateTaskCache {
	return NewNoopImmediateTaskCache()
}

func (n noopTaskCacheFactoryImpl) GetOrCreateScheduledTaskCache(shardID int32, category tasks.Category) ScheduledTaskCache {
	return NewNoopScheduledTaskCache()
}

func (n noopTaskCacheFactoryImpl) Stop() {
}

func (n noopTaskCacheFactoryImpl) InvalidateAllCaches(shardID int32) {
}

func NewTaskCacheFactory(logger log.Logger, metricsHandler metrics.Handler, cacheCapacity int) TaskCacheFactory {
	f := &taskCacheFactoryImpl{
		logger:                logger,
		metricsHandler:        metricsHandler,
		cacheCapacity:         cacheCapacity,
		transferTaskCaches:    make(map[int32]ImmediateTaskCache),
		visibilityTaskCaches:  make(map[int32]ImmediateTaskCache),
		replicationTaskCaches: make(map[int32]ImmediateTaskCache),
		timerTaskCaches:       make(map[int32]ScheduledTaskCache),
	}
	metricsReporter := cacheMetricsReporter{
		handler:        metricsHandler,
		reportInterval: 30 * time.Second,
		quit:           make(chan struct{}),
		logger:         logger,
		impl:           f,
	}
	f.metricsReporter = metricsReporter
	metricsReporter.Start()
	return f
}

func (f *taskCacheFactoryImpl) GetOrCreateImmediateTaskCache(shardID int32, category tasks.Category) ImmediateTaskCache {
	f.m.Lock()
	defer f.m.Unlock()

	var lookup map[int32]ImmediateTaskCache
	switch category {
	case tasks.CategoryTransfer:
		lookup = f.transferTaskCaches
	case tasks.CategoryVisibility:
		lookup = f.visibilityTaskCaches
	case tasks.CategoryReplication:
		lookup = f.visibilityTaskCaches
	default:
		panic("unknown immediate task category")
	}
	if _, ok := lookup[shardID]; !ok {
		lookup[shardID] = NewImmediateTaskCache(f.logger, f.metricsHandler.WithTags(metrics.TaskCategoryTag(category.Name())), f.cacheCapacity)
	}
	return lookup[shardID]
}

func (f *taskCacheFactoryImpl) GetOrCreateScheduledTaskCache(shardID int32, category tasks.Category) ScheduledTaskCache {
	f.m.Lock()
	defer f.m.Unlock()

	var lookup map[int32]ScheduledTaskCache
	switch category {
	case tasks.CategoryTimer:
		lookup = f.timerTaskCaches
	default:
		panic("unknown scheduled task category")
	}
	if _, ok := lookup[shardID]; !ok {
		lookup[shardID] = NewScheduledTaskCache(f.logger, f.metricsHandler.WithTags(metrics.TaskCategoryTag(category.Name())), f.cacheCapacity)
	}
	return lookup[shardID]
}

func (f *taskCacheFactoryImpl) InvalidateAllCaches(shardID int32) {
	f.m.Lock()
	defer f.m.Unlock()

	if c, ok := f.transferTaskCaches[shardID]; ok {
		c.Invalidate()
	}
	if c, ok := f.visibilityTaskCaches[shardID]; ok {
		c.Invalidate()
	}
	if c, ok := f.replicationTaskCaches[shardID]; ok {
		c.Invalidate()
	}
	if c, ok := f.timerTaskCaches[shardID]; ok {
		c.Invalidate()
	}
}

func (f *taskCacheFactoryImpl) Stop() {
	f.metricsReporter.Stop()
}

func (f *taskCacheFactoryImpl) getStatistics() Statistics {
	f.m.Lock()
	defer f.m.Unlock()

	total := Statistics{}
	add := func(s Statistics) {
		total.TotalTasksCount += s.TotalTasksCount
		total.PendingTasksCount += s.PendingTasksCount
		total.FutureTasksCount += s.FutureTasksCount
		total.TasksCount += s.TasksCount
	}
	for _, c := range f.transferTaskCaches {
		add(c.Statistics())
	}
	for _, c := range f.visibilityTaskCaches {
		add(c.Statistics())
	}
	for _, c := range f.replicationTaskCaches {
		add(c.Statistics())
	}
	for _, c := range f.timerTaskCaches {
		add(c.Statistics())
	}
	return total
}

type cacheMetricsReporter struct {
	handler        metrics.Handler
	reportInterval time.Duration
	started        int32
	quit           chan struct{}
	logger         log.Logger
	impl           *taskCacheFactoryImpl
}

func (r *cacheMetricsReporter) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}
	r.report()
	go func() {
		ticker := time.NewTicker(r.reportInterval)
		for {
			select {
			case <-ticker.C:
				r.report()
			case <-r.quit:
				ticker.Stop()
				return
			}
		}
	}()
	r.logger.Info("cacheMetricsReporter started")
}

func (r *cacheMetricsReporter) Stop() {
	close(r.quit)
	r.logger.Info("cacheMetricsReporter stopped")
}

func (r *cacheMetricsReporter) report() {
	s := r.impl.getStatistics()
	if r.handler != nil {
		r.handler.Gauge(futureTasksGauge.Name()).Record(float64(s.FutureTasksCount))
		r.handler.Gauge(pendingTasksGauge.Name()).Record(float64(s.PendingTasksCount))
		r.handler.Gauge(totalTasksGauge.Name()).Record(float64(s.TotalTasksCount))
	} else {
		fmt.Printf("handler: %s, STATISTICS: %+v\n", r.handler, s)
	}
}

type eventsCache struct {
	taskCacheFactory TaskCacheFactory
}

func (d *eventsCache) Invalidate(ctx context.Context, shardID int32) error {
	d.taskCacheFactory.InvalidateAllCaches(shardID)
	return nil
}

func (d *eventsCache) Put(ctx context.Context, shardID int32, tsss ...map[tasks.Category][]persistence.InternalHistoryTask) error {
	res := make(map[tasks.Category][]persistence.InternalHistoryTask)

	for _, tss := range tsss {
		for category, ts := range tss {
			res[category] = append(res[category], ts...)
		}
	}

	for category, ts := range res {
		switch category.Type() {
		case tasks.CategoryTypeImmediate:
			_ = d.taskCacheFactory.GetOrCreateImmediateTaskCache(shardID, category).Put(ts)
		case tasks.CategoryTypeScheduled:
			_ = d.taskCacheFactory.GetOrCreateScheduledTaskCache(shardID, category).Put(ts)
		}
	}
	return nil
}

func NewEventsCache(taskCacheFactory TaskCacheFactory) executor.EventsCache {
	return &eventsCache{taskCacheFactory: taskCacheFactory}
}
