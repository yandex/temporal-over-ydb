package persistencelite

import (
	"github.com/yandex/temporal-over-ydb/persistencelite/rows"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

func createIdentifiedEventRows(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[int64]*commonpb.DataBlob) []rows.Upsertable {
	rv := make([]rows.Upsertable, 0, len(events))
	for eventID, blob := range events {
		rv = append(rv, &rows.IdentifiedEventRow{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			WorkflowID:   workflowID,
			RunID:        runID,
			EventType:    eventType,
			EventID:      eventID,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
	}
	return rv
}

func createNamedEventRows(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[string]*commonpb.DataBlob) []rows.Upsertable {
	rv := make([]rows.Upsertable, 0, len(events))
	for eventName, blob := range events {
		rv = append(rv, &rows.NamedEventRow{
			ShardID:      shardID,
			NamespaceID:  namespaceID,
			WorkflowID:   workflowID,
			RunID:        runID,
			EventType:    eventType,
			EventName:    eventName,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
	}
	return rv
}

func createSignalRequestedRows(shardID int32, namespaceID, workflowID, runID string, signalRequestedIDs map[string]struct{}) []rows.Upsertable {
	rv := make([]rows.Upsertable, 0, len(signalRequestedIDs))
	for signalRequestedID := range signalRequestedIDs {
		rv = append(rv, &rows.SignalRequestedRow{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventName:   signalRequestedID,
		})
	}
	return rv
}

func createBufferedEventRow(shardID int32, namespaceID, workflowID, runID string, bufferedEventID string, bufferedEvent *commonpb.DataBlob) rows.Upsertable {
	return &rows.NamedEventRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		EventType:    rows.EventTypeBufferedEvent,
		EventName:    bufferedEventID,
		Data:         bufferedEvent.Data,
		DataEncoding: bufferedEvent.EncodingType.String(),
	}
}

func createHistoryTaskRows(shardID int32, category tasks.Category, historyTasks []p.InternalHistoryTask) []rows.Upsertable {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	rv := make([]rows.Upsertable, 0, len(historyTasks))
	for _, t := range historyTasks {
		var r rows.Upsertable
		if isScheduledTask {
			r = &rows.ScheduledTaskRow{
				ShardID:      shardID,
				CategoryID:   category.ID(),
				ID:           t.Key.TaskID,
				VisibilityTS: t.Key.FireTime,
				Data:         t.Blob.Data,
				DataEncoding: t.Blob.EncodingType.String(),
			}
		} else {
			r = &rows.ImmediateTaskRow{
				ShardID:      shardID,
				CategoryID:   category.ID(),
				ID:           t.Key.TaskID,
				Data:         t.Blob.Data,
				DataEncoding: t.Blob.EncodingType.String(),
			}
		}
		rv = append(rv, r)
	}
	return rv
}

func createTaskRows(shardID int32, insertTasks map[tasks.Category][]p.InternalHistoryTask) []rows.Upsertable {
	rv := make([]rows.Upsertable, 0, len(insertTasks))
	for category, tasksByCategory := range insertTasks {
		rv = append(rv, createHistoryTaskRows(shardID, category, tasksByCategory)...)
	}
	return rv
}

func handleWorkflowSnapshotBatchAsNew(shardID int32, workflowSnapshot *p.InternalWorkflowSnapshot) []rows.Upsertable {
	namespaceID := workflowSnapshot.NamespaceID
	workflowID := workflowSnapshot.WorkflowID
	runID := workflowSnapshot.RunID
	rs := createTaskRows(shardID, workflowSnapshot.Tasks)
	rs = append(rs, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeActivity, workflowSnapshot.ActivityInfos)...)
	rs = append(rs, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeChildExecution, workflowSnapshot.ChildExecutionInfos)...)
	rs = append(rs, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeRequestCancel, workflowSnapshot.RequestCancelInfos)...)
	rs = append(rs, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeSignal, workflowSnapshot.SignalInfos)...)
	rs = append(rs, createNamedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeTimer, workflowSnapshot.TimerInfos)...)
	rs = append(rs, createSignalRequestedRows(shardID, namespaceID, workflowID, runID, workflowSnapshot.SignalRequestedIDs)...)
	return rs
}

func createUpsertExecutionRowForSnapshot(shardID int32, snapshot *p.InternalWorkflowSnapshot) rows.Upsertable {
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

func createUpsertExecutionRowForMutation(shardID int32, mutation *p.InternalWorkflowMutation) rows.Upsertable {
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
) rows.Upsertable {
	return &rows.WorkflowRow{
		ShardID:                shardID,
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		RunID:                  runID,
		Execution:              executionInfoBlob.Data,
		ExecutionEncoding:      executionInfoBlob.EncodingType.String(),
		ExecutionState:         executionStateBlob.Data,
		ExecutionStateEncoding: executionStateBlob.EncodingType.String(),
		Checksum:               checksumBlob.Data,
		ChecksumEncoding:       checksumBlob.EncodingType.String(),
		NextEventID:            nextEventID,
		DBRecordVersion:        dbRecordVersion,
	}
}

func handleWorkflowMutationBatchAsNew(shardID int32, bufferedEventID string, workflowMutation *p.InternalWorkflowMutation) (ins []rows.Upsertable, del []rows.Deletable) {
	namespaceID := workflowMutation.NamespaceID
	workflowID := workflowMutation.WorkflowID
	runID := workflowMutation.RunID

	rowsToUpsertCap := len(workflowMutation.UpsertActivityInfos) +
		len(workflowMutation.UpsertTimerInfos) +
		len(workflowMutation.UpsertChildExecutionInfos) +
		len(workflowMutation.UpsertRequestCancelInfos) +
		len(workflowMutation.UpsertSignalInfos) +
		len(workflowMutation.UpsertSignalRequestedIDs)
	if workflowMutation.NewBufferedEvents != nil {
		rowsToUpsertCap += 1
	}
	eventKeysToRemoveCap := len(workflowMutation.DeleteActivityInfos) + len(workflowMutation.DeleteTimerInfos) + len(workflowMutation.DeleteChildExecutionInfos) + len(workflowMutation.DeleteRequestCancelInfos) + len(workflowMutation.DeleteSignalInfos) + len(workflowMutation.DeleteSignalRequestedIDs)

	ins = make([]rows.Upsertable, 0, rowsToUpsertCap)
	ins = append(ins, createTaskRows(shardID, workflowMutation.Tasks)...)
	ins = append(ins, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeActivity, workflowMutation.UpsertActivityInfos)...)
	ins = append(ins, createNamedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeTimer, workflowMutation.UpsertTimerInfos)...)
	ins = append(ins, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeChildExecution, workflowMutation.UpsertChildExecutionInfos)...)
	ins = append(ins, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeRequestCancel, workflowMutation.UpsertRequestCancelInfos)...)
	ins = append(ins, createIdentifiedEventRows(shardID, namespaceID, workflowID, runID, rows.EventTypeSignal, workflowMutation.UpsertSignalInfos)...)
	ins = append(ins, createSignalRequestedRows(shardID, namespaceID, workflowID, runID, workflowMutation.UpsertSignalRequestedIDs)...)
	if workflowMutation.NewBufferedEvents != nil {
		ins = append(ins, createBufferedEventRow(shardID, namespaceID, workflowID, runID, bufferedEventID, workflowMutation.NewBufferedEvents))
	}

	del = make([]rows.Deletable, 0, eventKeysToRemoveCap)
	del = append(del, eventsMapInt64ToYDBKeys(shardID, namespaceID, workflowID, runID, rows.EventTypeActivity, workflowMutation.DeleteActivityInfos)...)
	del = append(del, eventsMapStringToYDBKeys(shardID, namespaceID, workflowID, runID, rows.EventTypeTimer, workflowMutation.DeleteTimerInfos)...)
	del = append(del, eventsMapInt64ToYDBKeys(shardID, namespaceID, workflowID, runID, rows.EventTypeChildExecution, workflowMutation.DeleteChildExecutionInfos)...)
	del = append(del, eventsMapInt64ToYDBKeys(shardID, namespaceID, workflowID, runID, rows.EventTypeRequestCancel, workflowMutation.DeleteRequestCancelInfos)...)
	del = append(del, eventsMapInt64ToYDBKeys(shardID, namespaceID, workflowID, runID, rows.EventTypeSignal, workflowMutation.DeleteSignalInfos)...)
	del = append(del, signalRequestedIDsToYDBKeys(namespaceID, workflowID, runID, workflowMutation.DeleteSignalRequestedIDs)...)

	return ins, del
}

func eventsMapInt64ToYDBKeys(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[int64]struct{}) []rows.Deletable {
	rv := make([]rows.Deletable, 0, len(events))
	for eventID := range events {
		rv = append(rv, rows.IdentifiedEventKey{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventType:   eventType,
			EventID:     eventID,
		})
	}
	return rv
}

func eventsMapStringToYDBKeys(shardID int32, namespaceID, workflowID, runID string, eventType int32, events map[string]struct{}) []rows.Deletable {
	rv := make([]rows.Deletable, 0, len(events))
	for eventName := range events {
		rv = append(rv, rows.NamedEventKey{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventType:   eventType,
			EventName:   eventName,
		})
	}
	return rv
}

func signalRequestedIDsToYDBKeys(namespaceID, workflowID, runID string, signalRequestedIDs map[string]struct{}) []rows.Deletable {
	rv := make([]rows.Deletable, 0, len(signalRequestedIDs))
	for signalRequestedID := range signalRequestedIDs {
		rv = append(rv, rows.NamedEventKey{
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
			EventType:   rows.EventTypeSignalRequested,
			EventName:   signalRequestedID,
		})
	}
	return rv
}
