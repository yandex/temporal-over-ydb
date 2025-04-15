package cache

import "go.temporal.io/server/common/metrics"

var prefix = "tasks_cache_"
var hitCounter = metrics.NewCounterDef(prefix + "hit")
var emptyHitCounter = metrics.NewCounterDef(prefix + "empty_hit")
var missCounter = metrics.NewCounterDef(prefix + "miss")
var tasksPutCounter = metrics.NewCounterDef(prefix + "tasks_put")
var futureTasksPutCounter = metrics.NewCounterDef(prefix + "future_tasks_put")
var tasksReadCounter = metrics.NewCounterDef(prefix + "tasks_read")
var writeErrorCounter = metrics.NewCounterDef(prefix + "write_error")
var readErrorCounter = metrics.NewCounterDef(prefix + "read_error")

var futureTasksGauge = metrics.NewGaugeDef(prefix + "future_tasks")
var pendingTasksGauge = metrics.NewGaugeDef(prefix + "pending_tasks")
var totalTasksGauge = metrics.NewGaugeDef(prefix + "tasks")
