package persistence

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

func createIdentifiedEventRows(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[int64]*commonpb.DataBlob) []types.Value {
	rv := make([]types.Value, 0, len(events))
	for eventID, blob := range events {
		rv = append(rv, createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id":  types.UTF8Value(namespaceID),
			"workflow_id":   types.UTF8Value(workflowID),
			"run_id":        types.UTF8Value(runID),
			"event_type":    types.Int32Value(eventType),
			"event_id":      types.Int64Value(eventID),
			"data":          types.BytesValue(blob.Data),
			"data_encoding": types.UTF8Value(blob.EncodingType.String()),
		}))
	}
	return rv
}

func createNamedEventRows(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[string]*commonpb.DataBlob) []types.Value {
	rv := make([]types.Value, 0, len(events))
	for eventName, blob := range events {
		rv = append(rv, createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id":  types.UTF8Value(namespaceID),
			"workflow_id":   types.UTF8Value(workflowID),
			"run_id":        types.UTF8Value(runID),
			"event_type":    types.Int32Value(eventType),
			"event_name":    types.UTF8Value(eventName),
			"data":          types.BytesValue(blob.Data),
			"data_encoding": types.UTF8Value(blob.EncodingType.String()),
		}))
	}
	return rv
}

func createSignalRequestedRows(shardID int32, namespaceID, workflowID, runID string, signalRequestedIDs map[string]struct{}) []types.Value {
	rv := make([]types.Value, 0, len(signalRequestedIDs))
	for signalRequestedID := range signalRequestedIDs {
		rv = append(rv, createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id": types.UTF8Value(namespaceID),
			"workflow_id":  types.UTF8Value(workflowID),
			"run_id":       types.UTF8Value(runID),
			"event_type":   types.Int32Value(eventTypeSignalRequested),
			"event_name":   types.UTF8Value(signalRequestedID),
		}))
	}
	return rv
}

func createBufferedEventRow(shardID int32, namespaceID, workflowID, runID string, bufferedEventID string, bufferedEvent *commonpb.DataBlob) types.Value {
	return createExecutionsTableRow(shardID, map[string]types.Value{
		"namespace_id":  types.UTF8Value(namespaceID),
		"workflow_id":   types.UTF8Value(workflowID),
		"run_id":        types.UTF8Value(runID),
		"event_type":    types.Int32Value(eventTypeBufferedEvent),
		"event_name":    types.OptionalValue(types.UTF8Value(bufferedEventID)),
		"data":          types.OptionalValue(types.BytesValue(bufferedEvent.Data)),
		"data_encoding": types.OptionalValue(types.UTF8Value(bufferedEvent.EncodingType.String())),
	})
}

func createTransferTaskRows(shardID int32, transferTasks []p.InternalHistoryTask, categoryID int32) []types.Value {
	rv := make([]types.Value, 0, len(transferTasks))
	for _, t := range transferTasks {
		rv = append(rv, createExecutionsTableRow(shardID, map[string]types.Value{
			"task_category_id": types.Int32Value(categoryID),
			"task_id":          types.Int64Value(t.Key.TaskID),
			"data":             types.BytesValue(t.Blob.Data),
			"data_encoding":    types.UTF8Value(t.Blob.EncodingType.String()),
		}))
	}
	return rv
}

func createTaskWithVisibilityRows(shardID int32, timerTasks []p.InternalHistoryTask, categoryID int32) []types.Value {
	rv := make([]types.Value, 0, len(timerTasks))
	for _, t := range timerTasks {
		rv = append(rv, createExecutionsTableRow(shardID, map[string]types.Value{
			"task_category_id":   types.Int32Value(categoryID),
			"task_id":            types.Int64Value(t.Key.TaskID),
			"data":               types.BytesValue(t.Blob.Data),
			"data_encoding":      types.UTF8Value(t.Blob.EncodingType.String()),
			"task_visibility_ts": types.TimestampValueFromTime(ToYDBDateTime(t.Key.FireTime)),
		}))
	}
	return rv
}

func createHistoryTaskRows(shardID int32, category tasks.Category, historyTasks []p.InternalHistoryTask) []types.Value {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	rv := make([]types.Value, 0, len(historyTasks))
	for _, t := range historyTasks {
		vs := map[string]types.Value{
			"task_category_id": types.Int32Value(category.ID()),
			"task_id":          types.Int64Value(t.Key.TaskID),
			"data":             types.BytesValue(t.Blob.Data),
			"data_encoding":    types.UTF8Value(t.Blob.EncodingType.String()),
		}
		if isScheduledTask {
			vs["task_visibility_ts"] = types.TimestampValueFromTime(t.Key.FireTime)
		}
		rv = append(rv, createExecutionsTableRow(shardID, vs))
	}
	return rv
}

func createTaskRows(shardID int32, insertTasks map[tasks.Category][]p.InternalHistoryTask) []types.Value {
	rv := make([]types.Value, 0, len(insertTasks))
	for category, tasksByCategory := range insertTasks {
		switch category.ID() {
		case tasks.CategoryIDTransfer:
			rv = append(rv, createTransferTaskRows(shardID, tasksByCategory, tasks.CategoryIDTransfer)...)
		case tasks.CategoryIDTimer:
			rv = append(rv, createTaskWithVisibilityRows(shardID, tasksByCategory, tasks.CategoryIDTimer)...)
		case tasks.CategoryIDVisibility:
			rv = append(rv, createTransferTaskRows(shardID, tasksByCategory, tasks.CategoryIDVisibility)...)
		case tasks.CategoryIDReplication:
			rv = append(rv, createTransferTaskRows(shardID, tasksByCategory, tasks.CategoryIDReplication)...)
		default:
			rv = append(rv, createHistoryTaskRows(shardID, category, tasksByCategory)...)
		}
	}
	return rv
}

func handleWorkflowSnapshotBatchAsNew(shardID int32, workflowSnapshot *p.InternalWorkflowSnapshot) []types.Value {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID
	rows := createTaskRows(shardID, workflowSnapshot.Tasks)
	rows = append(rows, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeActivity, workflowSnapshot.ActivityInfos)...)
	rows = append(rows, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeChildExecution, workflowSnapshot.ChildExecutionInfos)...)
	rows = append(rows, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeRequestCancel, workflowSnapshot.RequestCancelInfos)...)
	rows = append(rows, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeSignal, workflowSnapshot.SignalInfos)...)
	rows = append(rows, createNamedEventRows(shardID, namespaceID, workflowID, runID, eventTypeTimer, workflowSnapshot.TimerInfos)...)
	rows = append(rows, createSignalRequestedRows(shardID, namespaceID, workflowID, runID, workflowSnapshot.SignalRequestedIDs)...)
	return rows
}

var nullRow = map[string]types.Value{
	"namespace_id":             types.NullValue(types.TypeUTF8),
	"workflow_id":              types.NullValue(types.TypeUTF8),
	"run_id":                   types.NullValue(types.TypeUTF8),
	"task_category_id":         types.NullValue(types.TypeInt32),
	"task_id":                  types.NullValue(types.TypeInt64),
	"event_type":               types.NullValue(types.TypeInt32),
	"event_id":                 types.NullValue(types.TypeInt64),
	"event_name":               types.NullValue(types.TypeUTF8),
	"task_visibility_ts":       types.NullValue(types.TypeTimestamp),
	"data":                     types.NullValue(types.TypeBytes),
	"data_encoding":            types.NullValue(types.TypeUTF8),
	"execution":                types.NullValue(types.TypeBytes),
	"execution_encoding":       types.NullValue(types.TypeUTF8),
	"execution_state":          types.NullValue(types.TypeBytes),
	"execution_state_encoding": types.NullValue(types.TypeUTF8),
	"checksum":                 types.NullValue(types.TypeBytes),
	"checksum_encoding":        types.NullValue(types.TypeUTF8),
	"next_event_id":            types.NullValue(types.TypeInt64),
	"db_record_version":        types.NullValue(types.TypeInt64),
	"current_run_id":           types.NullValue(types.TypeUTF8),
	"last_write_version":       types.NullValue(types.TypeInt64),
	"state":                    types.NullValue(types.TypeInt32),
}

func getNullStructFieldValues() map[string]types.StructValueOption {
	rv := make(map[string]types.StructValueOption, len(nullRow))
	for k, nullValue := range nullRow {
		rv[k] = types.StructFieldValue(k, nullValue)
	}
	return rv
}

var nullStructFieldValues = getNullStructFieldValues()

func createExecutionsTableRow(shardID int32, fields map[string]types.Value) types.Value {
	rv := make([]types.StructValueOption, 0, len(fields)+1)
	rv = append(rv, types.StructFieldValue("shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))))
	for k, nullValue := range nullStructFieldValues {
		if value, ok := fields[k]; ok {
			rv = append(rv, types.StructFieldValue(k, types.OptionalValue(value)))
		} else {
			rv = append(rv, nullValue)
		}
	}
	return types.StructValue(rv...)
}

func createUpsertExecutionRowForSnapshot(shardID int32, snapshot *p.InternalWorkflowSnapshot) types.Value {
	return createExecutionRow(
		shardID,
		snapshot.NamespaceID,
		snapshot.WorkflowID,
		snapshot.RunID,
		snapshot.ExecutionInfoBlob,
		snapshot.ExecutionStateBlob,
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		snapshot.Checksum,
	)
}

func createUpsertExecutionRowForMutation(shardID int32, mutation *p.InternalWorkflowMutation) types.Value {
	return createExecutionRow(
		shardID,
		mutation.NamespaceID,
		mutation.WorkflowID,
		mutation.RunID,
		mutation.ExecutionInfoBlob,
		mutation.ExecutionStateBlob,
		mutation.NextEventID,
		mutation.DBRecordVersion,
		mutation.Checksum,
	)
}

func createExecutionRow(
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionInfoBlob *commonpb.DataBlob,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
) types.Value {
	return createExecutionsTableRow(shardID, map[string]types.Value{
		"namespace_id":             types.UTF8Value(namespaceID),
		"workflow_id":              types.UTF8Value(workflowID),
		"run_id":                   types.UTF8Value(runID),
		"execution":                types.BytesValue(executionInfoBlob.Data),
		"execution_encoding":       types.UTF8Value(executionInfoBlob.EncodingType.String()),
		"execution_state":          types.BytesValue(executionStateBlob.Data),
		"execution_state_encoding": types.UTF8Value(executionStateBlob.EncodingType.String()),
		"checksum":                 types.BytesValue(checksumBlob.Data),
		"checksum_encoding":        types.UTF8Value(checksumBlob.EncodingType.String()),
		"next_event_id":            types.Int64Value(nextEventID),
		"db_record_version":        types.Int64Value(dbRecordVersion),
	})
}

func handleWorkflowMutationBatchAsNew(shardID int32, bufferedEventID string, workflowMutation *p.InternalWorkflowMutation) (rowsToUpsert []types.Value, eventKeysToRemove []types.Value) {
	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	rowsToUpsertCap := len(workflowMutation.UpsertActivityInfos) + len(workflowMutation.UpsertTimerInfos) + len(workflowMutation.UpsertChildExecutionInfos) + len(workflowMutation.UpsertRequestCancelInfos) + len(workflowMutation.UpsertSignalInfos) + len(workflowMutation.UpsertSignalRequestedIDs)
	if workflowMutation.NewBufferedEvents != nil {
		rowsToUpsertCap += 1
	}
	eventKeysToRemoveCap := len(workflowMutation.DeleteActivityInfos) + len(workflowMutation.DeleteTimerInfos) + len(workflowMutation.DeleteChildExecutionInfos) + len(workflowMutation.DeleteRequestCancelInfos) + len(workflowMutation.DeleteSignalInfos) + len(workflowMutation.DeleteSignalRequestedIDs)
	rowsToUpsert = make([]types.Value, 0, rowsToUpsertCap)

	rowsToUpsert = append(rowsToUpsert, createTaskRows(shardID, workflowMutation.Tasks)...)
	rowsToUpsert = append(rowsToUpsert, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeActivity, workflowMutation.UpsertActivityInfos)...)
	rowsToUpsert = append(rowsToUpsert, createNamedEventRows(shardID, namespaceID, workflowID, runID, eventTypeTimer, workflowMutation.UpsertTimerInfos)...)
	rowsToUpsert = append(rowsToUpsert, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeChildExecution, workflowMutation.UpsertChildExecutionInfos)...)
	rowsToUpsert = append(rowsToUpsert, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeRequestCancel, workflowMutation.UpsertRequestCancelInfos)...)
	rowsToUpsert = append(rowsToUpsert, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, eventTypeSignal, workflowMutation.UpsertSignalInfos)...)
	rowsToUpsert = append(rowsToUpsert, createSignalRequestedRows(shardID, namespaceID, workflowID, runID, workflowMutation.UpsertSignalRequestedIDs)...)
	if workflowMutation.NewBufferedEvents != nil {
		rowsToUpsert = append(rowsToUpsert, createBufferedEventRow(shardID, namespaceID, workflowID, runID, bufferedEventID, workflowMutation.NewBufferedEvents))
	}

	eventKeysToRemove = make([]types.Value, 0, eventKeysToRemoveCap)
	eventKeysToRemove = append(eventKeysToRemove, eventsMapInt64ToYDBKeys(namespaceID, workflowID, runID, eventTypeActivity, workflowMutation.DeleteActivityInfos)...)
	eventKeysToRemove = append(eventKeysToRemove, eventsMapStringToYDBKeys(namespaceID, workflowID, runID, eventTypeTimer, workflowMutation.DeleteTimerInfos)...)
	eventKeysToRemove = append(eventKeysToRemove, eventsMapInt64ToYDBKeys(namespaceID, workflowID, runID, eventTypeChildExecution, workflowMutation.DeleteChildExecutionInfos)...)
	eventKeysToRemove = append(eventKeysToRemove, eventsMapInt64ToYDBKeys(namespaceID, workflowID, runID, eventTypeRequestCancel, workflowMutation.DeleteRequestCancelInfos)...)
	eventKeysToRemove = append(eventKeysToRemove, eventsMapInt64ToYDBKeys(namespaceID, workflowID, runID, eventTypeSignal, workflowMutation.DeleteSignalInfos)...)
	eventKeysToRemove = append(eventKeysToRemove, signalRequestedIDsToYDBKeys(namespaceID, workflowID, runID, workflowMutation.DeleteSignalRequestedIDs)...)

	return rowsToUpsert, eventKeysToRemove
}

func eventsMapInt64ToYDBKeys(namespaceID, workflowID, runID string, eventType int32, events map[int64]struct{}) []types.Value {
	rv := make([]types.Value, 0, len(events))
	for eventID := range events {
		rv = append(rv, types.StructValue(
			types.StructFieldValue("namespace_id", types.UTF8Value(namespaceID)),
			types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
			types.StructFieldValue("run_id", types.UTF8Value(runID)),
			types.StructFieldValue("event_type", types.Int32Value(eventType)),
			types.StructFieldValue("event_id", types.OptionalValue(types.Int64Value(eventID))),
			types.StructFieldValue("event_name", types.NullValue(types.TypeUTF8)),
		))
	}
	return rv
}

func eventsMapStringToYDBKeys(namespaceID, workflowID, runID string, eventType int32, events map[string]struct{}) []types.Value {
	rv := make([]types.Value, 0, len(events))
	for eventName := range events {
		rv = append(rv, types.StructValue(
			types.StructFieldValue("namespace_id", types.UTF8Value(namespaceID)),
			types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
			types.StructFieldValue("run_id", types.UTF8Value(runID)),
			types.StructFieldValue("event_type", types.Int32Value(eventType)),
			types.StructFieldValue("event_id", types.NullValue(types.TypeInt64)),
			types.StructFieldValue("event_name", types.OptionalValue(types.UTF8Value(eventName))),
		))
	}
	return rv
}

func signalRequestedIDsToYDBKeys(namespaceID, workflowID, runID string, signalRequestedIDs map[string]struct{}) []types.Value {
	rv := make([]types.Value, 0, len(signalRequestedIDs))
	for signalRequestedID := range signalRequestedIDs {
		rv = append(rv, types.StructValue(
			types.StructFieldValue("namespace_id", types.UTF8Value(namespaceID)),
			types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
			types.StructFieldValue("run_id", types.UTF8Value(runID)),
			types.StructFieldValue("event_type", types.Int32Value(eventTypeSignalRequested)),
			types.StructFieldValue("event_id", types.NullValue(types.TypeInt64)),
			types.StructFieldValue("event_name", types.OptionalValue(types.UTF8Value(signalRequestedID))),
		))
	}
	return rv
}
