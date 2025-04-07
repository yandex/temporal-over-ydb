package persistence

const ydbPersistenceName = "ydb"

const (
	eventTypeActivity = iota
	eventTypeRequestCancel
	eventTypeSignal
	eventTypeTimer
	eventTypeChildExecution
	eventTypeBufferedEvent
	eventTypeSignalRequested
)

const (
	slowDeleteBatchSize = 10000
)
