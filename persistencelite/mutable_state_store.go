package persistencelite

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"
	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/cas"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	errorslite "github.com/yandex/temporal-over-ydb/persistencelite/errors"
	"github.com/yandex/temporal-over-ydb/persistencelite/rows"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	MutableStateStore struct {
		rqliteFactory conn.RqliteFactory
		logger        log.Logger
	}
)

func NewMutableStateStore(
	rqliteFactory conn.RqliteFactory,
	logger log.Logger,
) *MutableStateStore {
	return &MutableStateStore{
		rqliteFactory: rqliteFactory,
		logger:        logger,
	}
}

func (d *MutableStateStore) createWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	shardID := request.ShardID
	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := newWorkflow.NamespaceID
	workflowID := newWorkflow.WorkflowID
	runID := newWorkflow.RunID
	details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v", shardID, namespaceID, workflowID)

	var assertions []cas.Assertion

	assertions = append(assertions, &cas.ShardRangeCond{
		ShardID:        shardID,
		RangeIDEqualTo: request.RangeID,
	})

	rowsToUpsert := make([]rows.Upsertable, 0, 10)
	for _, req := range request.NewWorkflowNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}

	currentRow := &rows.CurrentExecutionRow{
		ShardID:                shardID,
		NamespaceID:            namespaceID,
		WorkflowID:             workflowID,
		CurrentRunID:           runID,
		ExecutionState:         newWorkflow.ExecutionStateBlob.Data,
		ExecutionStateEncoding: newWorkflow.ExecutionStateBlob.EncodingType.String(),
		LastWriteVersion:       lastWriteVersion,
		State:                  int32(newWorkflow.ExecutionState.State),
	}
	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// noop
	case p.CreateWorkflowModeUpdateCurrent:
		assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
			WorkflowID: workflowID,
			CurrentRunIDAndLastWriteVersionEqualTo: &cas.CurrentRunIDAndLastWriteVersionEqualToCond{
				LastWriteVersion: request.PreviousLastWriteVersion,
				CurrentRunID:     request.PreviousRunID,
			},
		})
		rowsToUpsert = append(rowsToUpsert, currentRow)
	case p.CreateWorkflowModeBrandNew:
		assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
			WorkflowID:   workflowID,
			MustNotExist: true,
		})
		rowsToUpsert = append(rowsToUpsert, currentRow)
	default:
		return nil, errorslite.NewInternalF("unknown mode: %v (%s)", request.Mode, details)
	}
	rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, &newWorkflow))
	rowsToUpsert = append(rowsToUpsert, handleWorkflowSnapshotBatchAsNew(shardID, &newWorkflow)...)
	assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
		WorkflowID:   workflowID,
		RunID:        runID,
		MustNotExist: true,
	})
	rql := d.rqliteFactory.GetClient(shardID)

	condID := uuid.New()
	if err = cas.ExecuteConditionally(ctx, rql, shardID, namespaceID, condID, assertions, rowsToUpsert, nil, nil); err != nil {
		return nil, err
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (d *MutableStateStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (resp *p.InternalGetWorkflowExecutionResponse, err error) {
	shardID := request.ShardID
	rql := d.rqliteFactory.GetClient(shardID)
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `
SELECT next_event_id, execution, execution_encoding, db_record_version, execution_state, execution_state_encoding, checksum, checksum_encoding
FROM executions
WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?
`,
			Arguments: []interface{}{shardID, request.NamespaceID, request.WorkflowID, request.RunID},
		},
		{
			Query: `
SELECT event_type, event_id, event_name, data, data_encoding
FROM events
WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?
`,
			Arguments: []interface{}{shardID, request.NamespaceID, request.WorkflowID, request.RunID},
		},
	})
	if err != nil {
		return nil, err
	}

	res := rs[0]
	if !res.Next() {
		return nil, errorslite.NewNotFoundF("workflow execution not found")
	}
	state, dbRecordVersion, err := scanMutableState(res)
	if err != nil {
		return nil, errorslite.NewInternalF("failed to scan execution: %s", err)
	}

	if res.Next() {
		return nil, errorslite.NewInternalF("duplicate workflow execution found")
	}

	state.ActivityInfos = make(map[int64]*commonpb.DataBlob)
	state.TimerInfos = make(map[string]*commonpb.DataBlob)
	state.ChildExecutionInfos = make(map[int64]*commonpb.DataBlob)
	state.RequestCancelInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalRequestedIDs = make([]string, 0)
	state.BufferedEvents = make([]*commonpb.DataBlob, 0)

	res = rs[1]
	for res.Next() {
		var eventID int64
		var eventName string
		var eventType int64
		var dataB64 string
		var encoding string
		if err = res.Scan(&eventType, &eventID, &eventName, &dataB64, &encoding); err != nil {
			return nil, errorslite.NewInternalF("failed to scan event: %s", err)
		}
		switch eventType {
		case rows.EventTypeActivity:
			state.ActivityInfos[eventID], err = conn.Base64ToBlob(dataB64, encoding)
		case rows.EventTypeTimer:
			state.TimerInfos[eventName], err = conn.Base64ToBlob(dataB64, encoding)
		case rows.EventTypeChildExecution:
			state.ChildExecutionInfos[eventID], err = conn.Base64ToBlob(dataB64, encoding)
		case rows.EventTypeRequestCancel:
			state.RequestCancelInfos[eventID], err = conn.Base64ToBlob(dataB64, encoding)
		case rows.EventTypeSignal:
			state.SignalInfos[eventID], err = conn.Base64ToBlob(dataB64, encoding)
		case rows.EventTypeSignalRequested:
			state.SignalRequestedIDs = append(state.SignalRequestedIDs, eventName)
		case rows.EventTypeBufferedEvent:
			var blob *commonpb.DataBlob
			blob, err = conn.Base64ToBlob(dataB64, encoding)
			state.BufferedEvents = append(state.BufferedEvents, blob)
		default:
			return nil, errorslite.NewInternalF("unknown event type: %d", eventType)
		}
		if err != nil {
			return nil, err
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

	if newWorkflow != nil {
		if updateWorkflow.NamespaceID != newWorkflow.NamespaceID {
			return errorslite.NewInternalF("cannot continue as new to another namespace")
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

	var assertions []cas.Assertion

	assertions = append(assertions, &cas.ShardRangeCond{
		ShardID:        shardID,
		RangeIDEqualTo: request.RangeID,
	})

	rowsToUpsert := make([]rows.Upsertable, 0, 10)
	for _, req := range request.NewWorkflowNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}
	for _, req := range request.UpdateWorkflowNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
			WorkflowID:             updateWorkflow.WorkflowID,
			CurrentRunIDNotEqualTo: updateWorkflow.RunID,
		})
	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			row := &rows.CurrentExecutionRow{
				ShardID:                shardID,
				NamespaceID:            namespaceID,
				WorkflowID:             newWorkflow.WorkflowID,
				CurrentRunID:           newWorkflow.RunID,
				ExecutionState:         newWorkflow.ExecutionStateBlob.Data,
				ExecutionStateEncoding: newWorkflow.ExecutionStateBlob.EncodingType.String(),
				LastWriteVersion:       newWorkflow.LastWriteVersion,
				State:                  int32(newWorkflow.ExecutionState.State),
			}
			rowsToUpsert = append(rowsToUpsert, row)
			assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
				WorkflowID:          newWorkflow.WorkflowID,
				CurrentRunIDEqualTo: updateWorkflow.RunID,
			})
		} else {
			executionStateBlob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}
			row := &rows.CurrentExecutionRow{
				ShardID:                shardID,
				NamespaceID:            namespaceID,
				WorkflowID:             updateWorkflow.WorkflowID,
				CurrentRunID:           updateWorkflow.RunID,
				ExecutionState:         executionStateBlob.Data,
				ExecutionStateEncoding: executionStateBlob.EncodingType.String(),
				LastWriteVersion:       updateWorkflow.LastWriteVersion,
				State:                  int32(updateWorkflow.ExecutionState.State),
			}
			rowsToUpsert = append(rowsToUpsert, row)
			assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
				WorkflowID:          updateWorkflow.WorkflowID,
				CurrentRunIDEqualTo: updateWorkflow.RunID,
			})
		}
	default:
		return errorslite.NewInternalF("unknown mode: %v", request.Mode)
	}

	expectedDBRecordVersion := updateWorkflow.DBRecordVersion - 1
	assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
		WorkflowID:           updateWorkflow.WorkflowID,
		RunID:                updateWorkflow.RunID,
		RecordVersionEqualTo: &expectedDBRecordVersion,
	})

	if newWorkflow != nil {
		assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
			WorkflowID:   newWorkflow.WorkflowID,
			RunID:        newWorkflow.RunID,
			MustNotExist: true,
		})
	}

	rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForMutation(shardID, &updateWorkflow))
	bufferedEventID := uuid.New()
	batchRowsToUpsert, rowsToDelete := handleWorkflowMutationBatchAsNew(shardID, bufferedEventID, &updateWorkflow)
	rowsToUpsert = append(rowsToUpsert, batchRowsToUpsert...)
	if newWorkflow != nil {
		rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, newWorkflow))
		rowsToUpsert = append(rowsToUpsert, handleWorkflowSnapshotBatchAsNew(shardID, newWorkflow)...)
	}

	condID := uuid.New()
	var otherStmts []gorqlite.ParameterizedStatement
	if updateWorkflow.ClearBufferedEvents {
		otherStmts = append(otherStmts, prepareDeleteBufferedEventsQuery(shardID, namespaceID, updateWorkflow.WorkflowID, updateWorkflow.RunID, condID))
	}
	rql := d.rqliteFactory.GetClient(shardID)

	return cas.ExecuteConditionally(ctx, rql, shardID, namespaceID, condID, assertions, rowsToUpsert, rowsToDelete, otherStmts)
}

func (d *MutableStateStore) conflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (err error) {
	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	if err = p.ValidateUpdateWorkflowStateStatus(resetWorkflow.ExecutionState.State, resetWorkflow.ExecutionState.Status); err != nil {
		return err
	}

	shardID := request.ShardID
	namespaceID := resetWorkflow.NamespaceID
	workflowID := resetWorkflow.WorkflowID

	rowsToUpsert := make([]rows.Upsertable, 0, 10)
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		rowsToUpsert = append(rowsToUpsert, getNodeRow(req))
		if req.IsNewBranch {
			rowsToUpsert = append(rowsToUpsert, getTreeRow(req))
		}
	}

	var assertions []cas.Assertion

	assertions = append(assertions, &cas.ShardRangeCond{
		ShardID:        shardID,
		RangeIDEqualTo: request.RangeID,
	})

	if request.Mode == p.ConflictResolveWorkflowModeBypassCurrent {
		assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
			WorkflowID:             workflowID,
			CurrentRunIDNotEqualTo: resetWorkflow.ExecutionState.RunId,
		})
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
			return errorslite.NewInternalF("WorkflowExecutionStateToBlob failed: %v", err)
		}

		row := &rows.CurrentExecutionRow{
			ShardID:                shardID,
			NamespaceID:            namespaceID,
			WorkflowID:             workflowID,
			CurrentRunID:           runID,
			ExecutionState:         executionStateBlob.Data,
			ExecutionStateEncoding: executionStateBlob.EncodingType.String(),
			LastWriteVersion:       lastWriteVersion,
			State:                  int32(state),
		}
		rowsToUpsert = append(rowsToUpsert, row)

		var currentRunID string
		if currentWorkflow != nil {
			currentRunID = currentWorkflow.ExecutionState.RunId
		} else {
			currentRunID = resetWorkflow.ExecutionState.RunId
		}
		assertions = append(assertions, &cas.CurrentWorkflowsRowCond{
			WorkflowID:          workflowID,
			CurrentRunIDEqualTo: currentRunID,
		})
	} else {
		return errorslite.NewInternalF("unknown mode: %v", request.Mode)
	}

	expectedResetWfDBRecordVersion := resetWorkflow.DBRecordVersion - 1
	assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
		WorkflowID:           resetWorkflow.WorkflowID,
		RunID:                resetWorkflow.RunID,
		RecordVersionEqualTo: &expectedResetWfDBRecordVersion,
	})
	if currentWorkflow != nil {
		expectedCurrentWfDBRecordVersion := currentWorkflow.DBRecordVersion - 1
		assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
			WorkflowID:           resetWorkflow.WorkflowID,
			RunID:                currentWorkflow.RunID,
			RecordVersionEqualTo: &expectedCurrentWfDBRecordVersion,
		})
	}

	rowsToUpsert = append(rowsToUpsert, handleWorkflowSnapshotBatchAsNew(shardID, &resetWorkflow)...)
	rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, &resetWorkflow))
	if newWorkflow != nil {
		rowsToUpsert = append(rowsToUpsert, handleWorkflowSnapshotBatchAsNew(shardID, newWorkflow)...)
		rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, newWorkflow))
	}

	condID := uuid.New()
	otherStmts := make([]gorqlite.ParameterizedStatement, 0, 3)

	var rowsToDelete []rows.Deletable
	if currentWorkflow != nil {
		bufferedEventID := uuid.New()
		batchRowsToUpsert, batchRowsToDelete := handleWorkflowMutationBatchAsNew(shardID, bufferedEventID, currentWorkflow)
		rowsToUpsert = append(rowsToUpsert, batchRowsToUpsert...)
		rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForMutation(shardID, currentWorkflow))
		rowsToDelete = append(rowsToDelete, batchRowsToDelete...)
		if currentWorkflow.ClearBufferedEvents {
			otherStmts = append(otherStmts, prepareDeleteBufferedEventsQuery(shardID, namespaceID, currentWorkflow.WorkflowID, currentWorkflow.RunID, condID))
		}
		otherStmts = append(otherStmts, prepareRemoveAllEventsForIDQuery(shardID, namespaceID, resetWorkflow.WorkflowID, resetWorkflow.RunID, condID))

	} else {
		otherStmts = append(otherStmts, prepareRemoveAllEventsForIDQuery(shardID, namespaceID, workflowID, resetWorkflow.RunID, condID))
	}
	rql := d.rqliteFactory.GetClient(shardID)
	return cas.ExecuteConditionally(ctx, rql, shardID, namespaceID, condID, assertions, rowsToUpsert, rowsToDelete, otherStmts)
}

func (d *MutableStateStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	shardID := request.ShardID
	rql := d.rqliteFactory.GetClient(shardID)
	args := []interface{}{shardID, request.NamespaceID, request.WorkflowID, request.RunID}
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     "DELETE FROM executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?",
			Arguments: args,
		},
		{
			Query:     "DELETE FROM events WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?",
			Arguments: args,
		},
	})
	return err
}

func (d *MutableStateStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	shardID := request.ShardID
	rql := d.rqliteFactory.GetClient(shardID)
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     "DELETE FROM current_executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND current_run_id = ?",
			Arguments: []interface{}{shardID, request.NamespaceID, request.WorkflowID, request.RunID},
		},
	})
	if err != nil {
		return errorslite.NewInternalF(err.Error())
	}
	return nil
}

func (d *MutableStateStore) GetCurrentExecution(
	ctx context.Context,
	request *p.GetCurrentExecutionRequest,
) (resp *p.InternalGetCurrentExecutionResponse, err error) {
	shardID := request.ShardID
	rql := d.rqliteFactory.GetClient(shardID)
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     "SELECT current_run_id, execution_state, execution_state_encoding FROM current_executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ?",
			Arguments: []interface{}{shardID, request.NamespaceID, request.WorkflowID},
		},
	})
	if err != nil {
		return nil, errorslite.NewInternalF(err.Error())
	}
	res := rs[0]

	if !res.Next() {
		return nil, errorslite.NewNotFoundF("current workflow execution not found")
	}

	var dataB64 string
	var encoding string
	var currentRunID string

	if err = res.Scan(&currentRunID, &dataB64, &encoding); err != nil {
		return nil, errorslite.NewInternalF("failed to scan current workflow execution row: %s", err)
	}

	executionStateBlob, err := conn.Base64ToBlob(dataB64, encoding)
	if err != nil {
		return nil, errorslite.NewInternalF("failed to decode execution state blob: %s", err)
	}

	executionState, err := serialization.WorkflowExecutionStateFromBlob(executionStateBlob.Data, executionStateBlob.EncodingType.String())
	if err != nil {
		return nil, errorslite.NewInternalF("failed to decode execution state from blob: %s", err)
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

	var assertions []cas.Assertion

	expectedDBRecordVersion := setSnapshot.DBRecordVersion - 1
	assertions = append(assertions, &cas.WorkflowExecutionsRowCond{
		WorkflowID:           setSnapshot.WorkflowID,
		RunID:                setSnapshot.RunID,
		RecordVersionEqualTo: &expectedDBRecordVersion,
	}, &cas.ShardRangeCond{
		ShardID:        shardID,
		RangeIDEqualTo: request.RangeID,
	})

	rowsToUpsert := handleWorkflowSnapshotBatchAsNew(shardID, &setSnapshot)
	rowsToUpsert = append(rowsToUpsert, createUpsertExecutionRowForSnapshot(shardID, &setSnapshot))

	condID := uuid.New()
	otherStmts := []gorqlite.ParameterizedStatement{
		prepareRemoveAllEventsForIDQuery(shardID, setSnapshot.NamespaceID, setSnapshot.WorkflowID, setSnapshot.RunID, condID),
	}

	rql := d.rqliteFactory.GetClient(shardID)
	return cas.ExecuteConditionally(ctx, rql, shardID, setSnapshot.NamespaceID, condID, assertions, rowsToUpsert, nil, otherStmts)
}

func (d *MutableStateStore) ListConcreteExecutions(
	_ context.Context,
	_ *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListConcreteExecutions is not implemented")
}

func scanMutableState(q gorqlite.QueryResult) (*p.InternalWorkflowMutableState, int64, error) {
	var executionDataB64 string
	var executionEncoding string
	var nextEventID int64
	var dbRecordVersion int64
	var stateDataB64 string
	var stateEncoding string
	var checksumDataB64 string
	var checksumEncoding string
	if err := q.Scan(&nextEventID, &executionDataB64, &executionEncoding, &dbRecordVersion, &stateDataB64, &stateEncoding, &checksumDataB64, &checksumEncoding); err != nil {
		return nil, 0, err
	}
	executionBlob, err := conn.Base64ToBlob(executionDataB64, executionEncoding)
	if err != nil {
		return nil, 0, fmt.Errorf("ffailed to decode execution: %w", err)
	}
	executionStateBlob, err := conn.Base64ToBlob(stateDataB64, stateEncoding)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode execution state: %w", err)
	}
	checksumBlob, err := conn.Base64ToBlob(checksumDataB64, checksumEncoding)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode checksum: %w", err)
	}
	return &p.InternalWorkflowMutableState{
		ExecutionInfo:  executionBlob,
		ExecutionState: executionStateBlob,
		Checksum:       checksumBlob,
		NextEventID:    nextEventID,
	}, dbRecordVersion, nil
}
