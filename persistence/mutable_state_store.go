package persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/pborman/uuid"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	MutableStateStore struct {
		client *xydb.Client
		logger log.Logger
	}
)

func NewMutableStateStore(
	client *xydb.Client,
	logger log.Logger,
) *MutableStateStore {
	return &MutableStateStore{
		client: client,
		logger: logger,
	}
}

func (d *MutableStateStore) createWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	shardID := request.ShardID
	rangeID := request.RangeID
	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := newWorkflow.NamespaceID
	workflowID := newWorkflow.WorkflowID
	runID := newWorkflow.RunID

	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v", shardID, namespaceID, workflowID)
			err = xydb.ConvertToTemporalError("CreateWorkflowExecution", err, details)
		}
	}()

	var currentWorkflowsRowC *currentWorkflowsRowCond
	var workflowExecutionsRowCs []workflowExecutionsRowCond
	var rows []types.Value

	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// noop

	case p.CreateWorkflowModeUpdateCurrent:
		currentWorkflowsRowC = &currentWorkflowsRowCond{
			workflowID: workflowID,
			currentRunIDAndLastWriteVersionEqualTo: &currentRunIDAndLastWriteVersionEqualToCond{
				lastWriteVersion: request.PreviousLastWriteVersion,
				currentRunID:     request.PreviousRunID,
			},
		}

		row := createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id":             types.UTF8Value(namespaceID),
			"workflow_id":              types.UTF8Value(workflowID),
			"run_id":                   currentExecutionRunID,
			"current_run_id":           types.UTF8Value(runID),
			"execution_state":          types.BytesValue(newWorkflow.ExecutionStateBlob.Data),
			"execution_state_encoding": types.UTF8Value(newWorkflow.ExecutionStateBlob.EncodingType.String()),
			"last_write_version":       types.Int64Value(lastWriteVersion),
			"state":                    types.Int32Value(int32(newWorkflow.ExecutionState.State)),
		})
		rows = append(rows, row)

	case p.CreateWorkflowModeBrandNew:
		currentWorkflowsRowC = &currentWorkflowsRowCond{
			workflowID:   workflowID,
			mustNotExist: true,
		}

		row := createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id":             types.UTF8Value(namespaceID),
			"workflow_id":              types.UTF8Value(workflowID),
			"run_id":                   currentExecutionRunID,
			"current_run_id":           types.UTF8Value(runID),
			"execution_state":          types.BytesValue(newWorkflow.ExecutionStateBlob.Data),
			"execution_state_encoding": types.UTF8Value(newWorkflow.ExecutionStateBlob.EncodingType.String()),
			"last_write_version":       types.Int64Value(lastWriteVersion),
			"state":                    types.Int32Value(int32(newWorkflow.ExecutionState.State)),
		})
		rows = append(rows, row)
	default:
		return nil, fmt.Errorf("unknown mode: %v", request.Mode)
	}

	workflowExecutionsRowCs = append(workflowExecutionsRowCs, workflowExecutionsRowCond{
		workflowID:   workflowID,
		runID:        runID,
		mustNotExist: true,
	})

	rows = append(rows, createUpsertExecutionRowForSnapshot(shardID, &newWorkflow))
	rows = append(rows, handleWorkflowSnapshotBatchAsNew(shardID, &newWorkflow)...)
	wqs := []*writeQuery{prepareUpsertRowsQuery(rows)}

	q := conditionsToReadQuery(shardID, namespaceID, rangeID, currentWorkflowsRowC, workflowExecutionsRowCs)
	err = d.client.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return executeOneReadAndManyWriteQueries(ctx, d.client, s, q, wqs)
	})
	if err != nil {
		return nil, err
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (d *MutableStateStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (resp *p.InternalGetWorkflowExecutionResponse, err error) {
	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v, run_id: %v",
				request.ShardID, request.NamespaceID, request.WorkflowID, request.RunID)
			err = xydb.ConvertToTemporalError("GetWorkflowExecution", err, details)
		}
	}()

	// With a single SELECT we read the execution row and all its events
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $namespace_id AS Utf8;
DECLARE $workflow_id AS Utf8;
DECLARE $run_id AS Utf8;

SELECT
execution, execution_encoding, execution_state, execution_state_encoding,
next_event_id, checksum, checksum_encoding, db_record_version,
event_type, event_id, event_name, data, data_encoding
FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id
AND run_id = $run_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
	), table.WithIdempotent())
	if err != nil {
		return
	}

	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()

	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}
	if !res.NextRow() {
		return nil, xydb.NewRootCauseError(serviceerror.NewNotFound, "workflow execution not found")
	}

	state, dbRecordVersion, err := scanMutableState(res)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, xydb.NewRootCauseError(serviceerror.NewNotFound, "workflow execution not found")
	}

	state.ActivityInfos = make(map[int64]*commonpb.DataBlob)
	state.TimerInfos = make(map[string]*commonpb.DataBlob)
	state.ChildExecutionInfos = make(map[int64]*commonpb.DataBlob)
	state.RequestCancelInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalRequestedIDs = make([]string, 0)
	state.BufferedEvents = make([]*commonpb.DataBlob, 0)

	for res.NextRow() {
		var eventID int64
		var eventName string
		var eventType int32
		var data []byte
		var encoding string
		if err = res.ScanNamed(
			named.OptionalWithDefault("event_type", &eventType),
			named.OptionalWithDefault("event_id", &eventID),
			named.OptionalWithDefault("event_name", &eventName),
			named.OptionalWithDefault("data", &data),
			named.OptionalWithDefault("data_encoding", &encoding),
		); err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		switch eventType {
		case eventTypeActivity:
			state.ActivityInfos[eventID] = p.NewDataBlob(data, encoding)
		case eventTypeTimer:
			state.TimerInfos[eventName] = p.NewDataBlob(data, encoding)
		case eventTypeChildExecution:
			state.ChildExecutionInfos[eventID] = p.NewDataBlob(data, encoding)
		case eventTypeRequestCancel:
			state.RequestCancelInfos[eventID] = p.NewDataBlob(data, encoding)
		case eventTypeSignal:
			state.SignalInfos[eventID] = p.NewDataBlob(data, encoding)
		case eventTypeSignalRequested:
			state.SignalRequestedIDs = append(state.SignalRequestedIDs, eventName)
		case eventTypeBufferedEvent:
			state.BufferedEvents = append(state.BufferedEvents, p.NewDataBlob(data, encoding))
		default:
			return nil, fmt.Errorf("unknown event type: %d", eventType)
		}
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbRecordVersion,
	}, nil
}

func (d *MutableStateStore) updateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (err error) {
	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v",
				request.ShardID, updateWorkflow.NamespaceID, updateWorkflow.WorkflowID)
			err = xydb.ConvertToTemporalError("UpdateWorkflowExecution", err, details)
		}
	}()

	if newWorkflow != nil {
		if updateWorkflow.NamespaceID != newWorkflow.NamespaceID {
			return errors.New("cannot continue as new to another namespace")
		}
		if err = p.ValidateCreateWorkflowStateStatus(newWorkflow.ExecutionState.State, newWorkflow.ExecutionState.Status); err != nil {
			return err
		}
	}
	// validate workflow state & close status
	if err = p.ValidateUpdateWorkflowStateStatus(updateWorkflow.ExecutionState.State, updateWorkflow.ExecutionState.Status); err != nil {
		return err
	}

	shardID := request.ShardID
	namespaceID := updateWorkflow.NamespaceID

	var currentWorkflowsRowC *currentWorkflowsRowCond
	var workflowExecutionsRowCs []workflowExecutionsRowCond

	rowsToUpsert := make([]types.Value, 0, 5)

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		currentWorkflowsRowC = &currentWorkflowsRowCond{
			workflowID:             updateWorkflow.WorkflowID,
			currentRunIDNotEqualTo: updateWorkflow.RunID,
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			row := createExecutionsTableRow(shardID, map[string]types.Value{
				"namespace_id":             types.UTF8Value(namespaceID),
				"workflow_id":              types.UTF8Value(newWorkflow.WorkflowID),
				"run_id":                   currentExecutionRunID,
				"current_run_id":           types.UTF8Value(newWorkflow.RunID),
				"execution_state":          types.BytesValue(newWorkflow.ExecutionStateBlob.Data),
				"execution_state_encoding": types.UTF8Value(newWorkflow.ExecutionStateBlob.EncodingType.String()),
				"last_write_version":       types.Int64Value(newWorkflow.LastWriteVersion),
				"state":                    types.Int32Value(int32(newWorkflow.ExecutionState.State)),
			})
			rowsToUpsert = append(rowsToUpsert, row)

			currentWorkflowsRowC = &currentWorkflowsRowCond{
				workflowID:          newWorkflow.WorkflowID,
				currentRunIDEqualTo: updateWorkflow.RunID,
			}
		} else {
			executionStateBlob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}

			row := createExecutionsTableRow(shardID, map[string]types.Value{
				"namespace_id":             types.UTF8Value(namespaceID),
				"workflow_id":              types.UTF8Value(updateWorkflow.WorkflowID),
				"run_id":                   currentExecutionRunID,
				"current_run_id":           types.UTF8Value(updateWorkflow.RunID),
				"execution_state":          types.BytesValue(executionStateBlob.Data),
				"execution_state_encoding": types.UTF8Value(executionStateBlob.EncodingType.String()),
				"last_write_version":       types.Int64Value(updateWorkflow.LastWriteVersion),
				"state":                    types.Int32Value(int32(updateWorkflow.ExecutionState.State)),
			})
			rowsToUpsert = append(rowsToUpsert, row)

			currentWorkflowsRowC = &currentWorkflowsRowCond{
				workflowID:          updateWorkflow.WorkflowID,
				currentRunIDEqualTo: updateWorkflow.RunID,
			}
		}
	default:
		return fmt.Errorf("unknown mode: %v", request.Mode)
	}

	expectedDBRecordVersion := updateWorkflow.DBRecordVersion - 1
	workflowExecutionsRowCs = append(workflowExecutionsRowCs, workflowExecutionsRowCond{
		workflowID:             updateWorkflow.WorkflowID,
		runID:                  updateWorkflow.RunID,
		dbRecordVersionEqualTo: &expectedDBRecordVersion,
	})

	if newWorkflow != nil {
		workflowExecutionsRowCs = append(workflowExecutionsRowCs, workflowExecutionsRowCond{
			workflowID:   newWorkflow.WorkflowID,
			runID:        newWorkflow.RunID,
			mustNotExist: true,
		})
	}

	rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForMutation(shardID, &updateWorkflow))
	bufferedEventID := uuid.New()
	batchRowsToUpsert, eventKeysToRemove := handleWorkflowMutationBatchAsNew(shardID, bufferedEventID, &updateWorkflow)
	rowsToUpsert = append(rowsToUpsert, batchRowsToUpsert...)
	if newWorkflow != nil {
		rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, newWorkflow))
		rowsToUpsert = append(rowsToUpsert, handleWorkflowSnapshotBatchAsNew(shardID, newWorkflow)...)
	}

	wqs := make([]*writeQuery, 0, 3)
	if updateWorkflow.ClearBufferedEvents {
		wqs = append(wqs, prepareDeleteBufferedEventsQuery(shardID, namespaceID, updateWorkflow.WorkflowID, updateWorkflow.RunID))
	}
	if len(eventKeysToRemove) > 0 {
		wqs = append(wqs, prepareDeleteEventRowsQuery(shardID, eventKeysToRemove))
	}
	wqs = append(wqs, prepareUpsertRowsQuery(rowsToUpsert))

	q := conditionsToReadQuery(shardID, namespaceID, request.RangeID, currentWorkflowsRowC, workflowExecutionsRowCs)
	return d.client.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return executeOneReadAndManyWriteQueries(ctx, d.client, s, q, wqs)
	})
}

func (d *MutableStateStore) conflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (err error) {
	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v",
				request.ShardID, resetWorkflow.NamespaceID, resetWorkflow.WorkflowID)
			err = xydb.ConvertToTemporalError("ConflictResolveWorkflowExecution", err, details)
		}
	}()

	if err = p.ValidateUpdateWorkflowStateStatus(resetWorkflow.ExecutionState.State, resetWorkflow.ExecutionState.Status); err != nil {
		return err
	}

	shardID := request.ShardID
	namespaceID := resetWorkflow.NamespaceID
	workflowID := resetWorkflow.WorkflowID

	var currentWorkflowsRowC *currentWorkflowsRowCond
	workflowExecutionsRowCs := make([]workflowExecutionsRowCond, 0, 2)
	rows := make([]types.Value, 0, 5)

	if request.Mode == p.ConflictResolveWorkflowModeBypassCurrent {
		currentWorkflowsRowC = &currentWorkflowsRowCond{
			workflowID:             workflowID,
			currentRunIDNotEqualTo: resetWorkflow.ExecutionState.RunId,
		}
	} else if request.Mode == p.ConflictResolveWorkflowModeUpdateCurrent {
		executionState := resetWorkflow.ExecutionState
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			executionState = newWorkflow.ExecutionState
			lastWriteVersion = newWorkflow.LastWriteVersion
		}
		runID := executionState.RunId
		createRequestID := executionState.CreateRequestId
		state := executionState.State
		status := executionState.Status

		executionStateBlob, err := serialization.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
			RunId:           runID,
			CreateRequestId: createRequestID,
			State:           state,
			Status:          status,
		})
		if err != nil {
			return fmt.Errorf("WorkflowExecutionStateToBlob failed: %v", err)
		}

		row := createExecutionsTableRow(shardID, map[string]types.Value{
			"namespace_id":             types.UTF8Value(namespaceID),
			"workflow_id":              types.UTF8Value(workflowID),
			"run_id":                   currentExecutionRunID,
			"current_run_id":           types.UTF8Value(runID),
			"execution_state":          types.BytesValue(executionStateBlob.Data),
			"execution_state_encoding": types.UTF8Value(executionStateBlob.EncodingType.String()),
			"last_write_version":       types.Int64Value(lastWriteVersion),
			"state":                    types.Int32Value(int32(state)),
		})
		rows = append(rows, row)

		var currentRunID string
		if currentWorkflow != nil {
			currentRunID = currentWorkflow.ExecutionState.RunId
		} else {
			currentRunID = resetWorkflow.ExecutionState.RunId
		}
		currentWorkflowsRowC = &currentWorkflowsRowCond{
			workflowID:          workflowID,
			currentRunIDEqualTo: currentRunID,
		}
	} else {
		return fmt.Errorf("unknown mode: %v", request.Mode)
	}

	expectedResetWfDBRecordVersion := resetWorkflow.DBRecordVersion - 1
	workflowExecutionsRowCs = append(workflowExecutionsRowCs, workflowExecutionsRowCond{
		workflowID:             resetWorkflow.WorkflowID,
		runID:                  resetWorkflow.RunID,
		dbRecordVersionEqualTo: &expectedResetWfDBRecordVersion,
	})
	if currentWorkflow != nil {
		expectedCurrentWfDBRecordVersion := currentWorkflow.DBRecordVersion - 1
		workflowExecutionsRowCs = append(workflowExecutionsRowCs, workflowExecutionsRowCond{
			workflowID:             resetWorkflow.WorkflowID,
			runID:                  currentWorkflow.RunID,
			dbRecordVersionEqualTo: &expectedCurrentWfDBRecordVersion,
		})
	}

	rows = append(rows, handleWorkflowSnapshotBatchAsNew(shardID, &resetWorkflow)...)
	if newWorkflow != nil {
		rows = append(rows, handleWorkflowSnapshotBatchAsNew(shardID, newWorkflow)...)
		rows = append(rows, createUpsertExecutionRowForSnapshot(shardID, newWorkflow))
	}
	rows = append(rows, createUpsertExecutionRowForSnapshot(shardID, &resetWorkflow))

	wqs := make([]*writeQuery, 0, 3)
	if currentWorkflow != nil {
		bufferedEventID := uuid.New()
		batchRowsToUpsert, eventKeysToRemove := handleWorkflowMutationBatchAsNew(shardID, bufferedEventID, currentWorkflow)
		rows = append(rows, batchRowsToUpsert...)
		rows = append(rows, createUpsertExecutionRowForMutation(shardID, currentWorkflow))
		if currentWorkflow.ClearBufferedEvents {
			wqs = append(wqs, prepareDeleteBufferedEventsForID1AndRemoveAllEventsForID2Query(
				shardID, namespaceID, workflowID, currentWorkflow.RunID, resetWorkflow.RunID,
			))
		} else {
			wqs = append(wqs, prepareRemoveAllEventsForIDQuery(shardID, namespaceID, workflowID, resetWorkflow.RunID))
		}
		if len(eventKeysToRemove) > 0 {
			wqs = append(wqs, prepareDeleteEventRowsQuery(shardID, eventKeysToRemove))
		}
	} else {
		wqs = append(wqs, prepareRemoveAllEventsForIDQuery(shardID, namespaceID, workflowID, resetWorkflow.RunID))
	}
	wqs = append(wqs, prepareUpsertRowsQuery(rows))

	q := conditionsToReadQuery(shardID, namespaceID, request.RangeID, currentWorkflowsRowC, workflowExecutionsRowCs)
	return d.client.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return executeOneReadAndManyWriteQueries(ctx, d.client, s, q, wqs)
	})
}

func (d *MutableStateStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $namespace_id AS Utf8;
DECLARE $workflow_id AS Utf8;
DECLARE $run_id AS Utf8;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id
AND run_id = $run_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
	))

	if err != nil {
		return xydb.ConvertToTemporalError("DeleteWorkflowExecution", err)
	}
	return nil
}

func (d *MutableStateStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $namespace_id AS Utf8;
DECLARE $workflow_id AS Utf8;
DECLARE $run_id AS Utf8;
DECLARE $current_run_id AS Utf8;

SELECT Ensure(current_run_id, current_run_id == $current_run_id, "CURRENT_RUN_ID_MISMATCH")
FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id
AND run_id = $run_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL
LIMIT 1;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id
AND run_id = $run_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
	`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", currentExecutionRunID),
		table.ValueParam("$current_run_id", types.UTF8Value(request.RunID)),
	))
	if err != nil {
		if xydb.IsPreconditionFailedAndContains(err, "CURRENT_RUN_ID_MISMATCH") {
			err = xydb.WrapErrorAsRootCause(&p.ConditionFailedError{
				Msg: fmt.Sprintf("DeleteCurrentWorkflowExecution: current_run_id mismatch (%s)", request.RunID),
			})
		}
		return xydb.ConvertToTemporalError("DeleteWorkflowCurrentRow", err)
	}
	return nil
}

func (d *MutableStateStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (resp *p.InternalGetCurrentExecutionResponse, err error) {
	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v", request.ShardID, request.NamespaceID, request.WorkflowID)
			err = xydb.ConvertToTemporalError("GetCurrentExecution", err, details)
		}
	}()
	query := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $namespace_id AS Utf8;
DECLARE $workflow_id AS Utf8;
DECLARE $run_id AS Utf8;

SELECT current_run_id, execution_state, execution_state_encoding
FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id
AND run_id = $run_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL
LIMIT 1;
`)
	res, err := d.client.Do(ctx, query, table.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", currentExecutionRunID),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()

	if err = xydb.EnsureOneRowCursor(ctx, res); err != nil {
		return nil, err
	}
	var data []byte
	var encoding string
	var currentRunID string

	if err = res.ScanWithDefaults(&currentRunID, &data, &encoding); err != nil {
		return nil, fmt.Errorf("failed to scan current workflow execution row: %w", err)
	}
	executionStateBlob := p.NewDataBlob(data, encoding)
	executionState, err := serialization.WorkflowExecutionStateFromBlob(executionStateBlob.Data, executionStateBlob.EncodingType.String())
	if err != nil {
		return nil, err
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          currentRunID,
		ExecutionState: executionState,
	}, nil
}

func (d *MutableStateStore) SetWorkflowExecution(ctx context.Context, request *p.InternalSetWorkflowExecutionRequest) (err error) {
	shardID := request.ShardID
	setSnapshot := request.SetWorkflowSnapshot

	if err = p.ValidateUpdateWorkflowStateStatus(setSnapshot.ExecutionState.State, setSnapshot.ExecutionState.Status); err != nil {
		return err
	}

	expectedDBRecordVersion := setSnapshot.DBRecordVersion - 1
	workflowExecutionsRowCs := []workflowExecutionsRowCond{
		{
			workflowID:             setSnapshot.WorkflowID,
			runID:                  setSnapshot.RunID,
			dbRecordVersionEqualTo: &expectedDBRecordVersion,
		},
	}

	rows := handleWorkflowSnapshotBatchAsNew(shardID, &setSnapshot)
	rows = append(rows, createUpsertExecutionRowForSnapshot(shardID, &setSnapshot))
	wqs := []*writeQuery{
		prepareRemoveAllEventsForIDQuery(shardID, setSnapshot.NamespaceID, setSnapshot.WorkflowID, setSnapshot.RunID),
		prepareUpsertRowsQuery(rows),
	}
	q := conditionsToReadQuery(shardID, setSnapshot.NamespaceID, request.RangeID, nil, workflowExecutionsRowCs)
	err = d.client.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return executeOneReadAndManyWriteQueries(ctx, d.client, s, q, wqs)
	})
	return xydb.ConvertToTemporalError("SetWorkflowExecution", err)
}

func (d *MutableStateStore) ListConcreteExecutions(
	ctx context.Context,
	request *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListConcreteExecutions is not implemented")
}

func scanMutableState(res result.Result) (*p.InternalWorkflowMutableState, int64, error) {
	var data []byte
	var encoding string
	var nextEventID int64
	var dbRecordVersion int64
	var stateData []byte
	var stateEncoding string
	var checksumData []byte
	var checksumEncoding string
	var eventID int64
	var eventName string
	if err := res.ScanNamed(
		named.OptionalWithDefault("next_event_id", &nextEventID),
		named.OptionalWithDefault("execution", &data),
		named.OptionalWithDefault("execution_encoding", &encoding),
		named.OptionalWithDefault("db_record_version", &dbRecordVersion),
		named.OptionalWithDefault("execution_state", &stateData),
		named.OptionalWithDefault("execution_state_encoding", &stateEncoding),
		named.OptionalWithDefault("checksum", &checksumData),
		named.OptionalWithDefault("checksum_encoding", &checksumEncoding),
		named.OptionalWithDefault("event_id", &eventID),
		named.OptionalWithDefault("event_name", &eventName),
	); err != nil {
		return nil, 0, fmt.Errorf("failed to scan execution: %w", err)
	}
	if eventID > 0 || len(eventName) > 0 {
		// expected an execution, scanned an event
		return nil, 0, nil
	}
	return &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(data, encoding),
		ExecutionState: p.NewDataBlob(stateData, stateEncoding),
		Checksum:       p.NewDataBlob(checksumData, checksumEncoding),
		NextEventID:    nextEventID,
	}, dbRecordVersion, nil
}
