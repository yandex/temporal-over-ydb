package mss

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"

	baseerrors "github.com/yandex/temporal-over-ydb/persistence/pkg/base/errors"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
)

type (
	BaseMutableStateStore struct {
		cache executor.EventsCache
	}
)

func NewBaseMutableStateStore(
	cache executor.EventsCache,
) *BaseMutableStateStore {
	return &BaseMutableStateStore{
		cache: cache,
	}
}

func (d *BaseMutableStateStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
	tf executor.TransactionFactory,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	transaction := tf.NewTransaction(request.ShardID)
	return d.CreateWorkflowExecutionWithinTransaction(ctx, request, transaction)
}

func (d *BaseMutableStateStore) CreateWorkflowExecutionWithinTransaction(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
	transaction executor.Transaction,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	defer func() {
		// TODO narrow this condition: we should invalidate only if we actually lost shard ownership
		if err != nil {
			_ = d.cache.Invalidate(ctx, request.ShardID)
		}
	}()

	shardID := request.ShardID
	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := primitives.MustParseUUID(newWorkflow.NamespaceID)
	workflowID := newWorkflow.WorkflowID
	runID := primitives.MustParseUUID(newWorkflow.RunID)

	transaction.AssertShard(false, request.RangeID)

	switch request.Mode {
	case p.CreateWorkflowModeBypassCurrent:
		// noop
	case p.CreateWorkflowModeUpdateCurrent:
		transaction.AssertCurrentWorkflow(
			true,
			namespaceID,
			workflowID,
			nil,
			nil,
			false,
			&executor.CurrentRunIDAndLastWriteVersion{
				LastWriteVersion: request.PreviousLastWriteVersion,
				CurrentRunID:     primitives.MustParseUUID(request.PreviousRunID),
			},
		)
		transaction.UpsertCurrentWorkflow(
			namespaceID, workflowID, runID,
			newWorkflow.ExecutionStateBlob, lastWriteVersion, newWorkflow.ExecutionState.State,
		)
	case p.CreateWorkflowModeBrandNew:
		transaction.AssertCurrentWorkflow(
			false,
			namespaceID,
			workflowID,
			nil,
			nil,
			true,
			nil,
		)
		transaction.UpsertCurrentWorkflow(
			namespaceID, workflowID, runID,
			newWorkflow.ExecutionStateBlob, lastWriteVersion, newWorkflow.ExecutionState.State,
		)
	default:
		return nil, baseerrors.NewInternalF("unknown mode: %v", request.Mode)
	}
	transaction.HandleWorkflowSnapshot(&newWorkflow)
	transaction.AssertWorkflowExecution(
		false,
		namespaceID,
		workflowID,
		runID,
		nil,
		true,
	)

	if err = transaction.Execute(ctx); err != nil {
		return nil, err
	} else {
		_ = d.cache.Put(ctx, shardID, newWorkflow.Tasks)
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}

func (d *BaseMutableStateStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
	tf executor.TransactionFactory,
) (err error) {
	transaction := tf.NewTransaction(request.ShardID)
	return d.UpdateWorkflowExecutionWithinTransaction(ctx, request, transaction)
}

func (d *BaseMutableStateStore) UpdateWorkflowExecutionWithinTransaction(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
	transaction executor.Transaction,
) (err error) {
	defer func() {
		// TODO narrow this condition: we should invalidate only if we actually lost shard ownership
		if err != nil {
			_ = d.cache.Invalidate(ctx, request.ShardID)
		}
	}()

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	if newWorkflow != nil {
		if updateWorkflow.NamespaceID != newWorkflow.NamespaceID {
			return baseerrors.NewInternalF("cannot continue as new to another namespace")
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
	namespaceID := primitives.MustParseUUID(updateWorkflow.NamespaceID)

	transaction.AssertShard(false, request.RangeID)

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		transaction.AssertCurrentWorkflow(
			true,
			namespaceID,
			updateWorkflow.WorkflowID,
			primitives.MustParseUUID(updateWorkflow.RunID),
			nil,
			false,
			nil,
		)
	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			transaction.UpsertCurrentWorkflow(
				namespaceID, newWorkflow.WorkflowID, primitives.MustParseUUID(newWorkflow.RunID),
				newWorkflow.ExecutionStateBlob, newWorkflow.LastWriteVersion, newWorkflow.ExecutionState.State,
			)

			transaction.AssertCurrentWorkflow(
				true,
				namespaceID,
				newWorkflow.WorkflowID,
				nil,
				primitives.MustParseUUID(updateWorkflow.RunID),
				false,
				nil,
			)
		} else {
			executionStateBlob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}
			transaction.UpsertCurrentWorkflow(
				namespaceID, updateWorkflow.WorkflowID, primitives.MustParseUUID(updateWorkflow.RunID),
				executionStateBlob, updateWorkflow.LastWriteVersion, updateWorkflow.ExecutionState.State,
			)

			transaction.AssertCurrentWorkflow(
				true,
				namespaceID,
				updateWorkflow.WorkflowID,
				nil,
				primitives.MustParseUUID(updateWorkflow.RunID),
				false,
				nil,
			)
		}
	default:
		return baseerrors.NewInternalF("unknown mode: %v", request.Mode)
	}

	expectedDBRecordVersion := updateWorkflow.DBRecordVersion - 1

	transaction.AssertWorkflowExecution(
		true,
		namespaceID,
		updateWorkflow.WorkflowID,
		primitives.MustParseUUID(updateWorkflow.RunID),
		&expectedDBRecordVersion,
		false,
	)

	if newWorkflow != nil {
		transaction.AssertWorkflowExecution(
			false,
			namespaceID,
			newWorkflow.WorkflowID,
			primitives.MustParseUUID(newWorkflow.RunID),
			nil,
			true,
		)
	}

	transaction.HandleWorkflowMutation(&updateWorkflow)
	if newWorkflow != nil {
		transaction.HandleWorkflowSnapshot(newWorkflow)
	}

	if updateWorkflow.ClearBufferedEvents {
		transaction.DeleteBufferedEvents(namespaceID, updateWorkflow.WorkflowID, primitives.MustParseUUID(updateWorkflow.RunID))
	}
	err = transaction.Execute(ctx)
	if err == nil {
		taskMaps := []map[tasks.Category][]p.InternalHistoryTask{updateWorkflow.Tasks}
		_ = d.cache.Put(ctx, shardID, updateWorkflow.Tasks)
		if newWorkflow != nil {
			taskMaps = append(taskMaps, newWorkflow.Tasks)
		}
		_ = d.cache.Put(ctx, shardID, taskMaps...)
	}
	return err
}

func (d *BaseMutableStateStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
	tf executor.TransactionFactory,
) (err error) {
	transaction := tf.NewTransaction(request.ShardID)
	return d.ConflictResolveWorkflowExecutionWithinTransaction(ctx, request, transaction)
}

func (d *BaseMutableStateStore) ConflictResolveWorkflowExecutionWithinTransaction(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
	transaction executor.Transaction,
) (err error) {
	defer func() {
		// TODO narrow this condition: we should invalidate only if we actually lost shard ownership
		if err != nil {
			_ = d.cache.Invalidate(ctx, request.ShardID)
		}
	}()

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	if err = p.ValidateUpdateWorkflowStateStatus(resetWorkflow.ExecutionState.State, resetWorkflow.ExecutionState.Status); err != nil {
		return err
	}

	shardID := request.ShardID
	namespaceID := primitives.MustParseUUID(resetWorkflow.NamespaceID)
	workflowID := resetWorkflow.WorkflowID

	transaction.AssertShard(false, request.RangeID)

	if request.Mode == p.ConflictResolveWorkflowModeBypassCurrent {
		transaction.AssertCurrentWorkflow(
			true,
			namespaceID,
			workflowID,
			primitives.MustParseUUID(resetWorkflow.ExecutionState.RunId),
			nil,
			false,
			nil,
		)
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
			return baseerrors.NewInternalF("WorkflowExecutionStateToBlob failed: %v", err)
		}

		var currentRunID string
		if currentWorkflow != nil {
			currentRunID = currentWorkflow.ExecutionState.RunId
		} else {
			currentRunID = resetWorkflow.ExecutionState.RunId
		}
		transaction.AssertCurrentWorkflow(
			true,
			namespaceID,
			workflowID,
			nil,
			primitives.MustParseUUID(currentRunID),
			false,
			nil,
		)
		transaction.UpsertCurrentWorkflow(
			namespaceID, workflowID, primitives.MustParseUUID(runID),
			executionStateBlob, lastWriteVersion, state,
		)
	} else {
		return baseerrors.NewInternalF("unknown mode: %v", request.Mode)
	}

	expectedResetWfDBRecordVersion := resetWorkflow.DBRecordVersion - 1
	transaction.AssertWorkflowExecution(
		true,
		namespaceID,
		resetWorkflow.WorkflowID,
		primitives.MustParseUUID(resetWorkflow.RunID),
		&expectedResetWfDBRecordVersion,
		false,
	)
	if currentWorkflow != nil {
		expectedCurrentWfDBRecordVersion := currentWorkflow.DBRecordVersion - 1
		transaction.AssertWorkflowExecution(
			true,
			namespaceID,
			resetWorkflow.WorkflowID,
			primitives.MustParseUUID(currentWorkflow.RunID),
			&expectedCurrentWfDBRecordVersion,
			false,
		)
	}

	transaction.HandleWorkflowSnapshot(&resetWorkflow)
	if newWorkflow != nil {
		transaction.HandleWorkflowSnapshot(newWorkflow)
	}

	if currentWorkflow != nil {
		transaction.HandleWorkflowMutation(currentWorkflow)
		if currentWorkflow.ClearBufferedEvents {
			transaction.DeleteBufferedEvents(namespaceID, currentWorkflow.WorkflowID, primitives.MustParseUUID(currentWorkflow.RunID))
		}
		transaction.DeleteStateItems(namespaceID, resetWorkflow.WorkflowID, primitives.MustParseUUID(resetWorkflow.RunID))
	} else {
		transaction.DeleteStateItems(namespaceID, workflowID, primitives.MustParseUUID(resetWorkflow.RunID))
	}

	err = transaction.Execute(ctx)
	if err == nil {
		taskMaps := []map[tasks.Category][]p.InternalHistoryTask{resetWorkflow.Tasks}
		if newWorkflow != nil {
			taskMaps = append(taskMaps, newWorkflow.Tasks)
		}
		if currentWorkflow != nil {
			taskMaps = append(taskMaps, currentWorkflow.Tasks)
		}
		_ = d.cache.Put(ctx, shardID, taskMaps...)
	}
	return err
}

func (d *BaseMutableStateStore) SetWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
	tf executor.TransactionFactory,
) (err error) {
	transaction := tf.NewTransaction(request.ShardID)
	return d.SetWorkflowExecutionWithinTransaction(ctx, request, transaction)
}

func (d *BaseMutableStateStore) SetWorkflowExecutionWithinTransaction(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
	transaction executor.Transaction,
) (err error) {
	setSnapshot := request.SetWorkflowSnapshot
	namespaceID := primitives.MustParseUUID(setSnapshot.NamespaceID)
	runID := primitives.MustParseUUID(setSnapshot.RunID)

	if err = p.ValidateUpdateWorkflowStateStatus(setSnapshot.ExecutionState.State, setSnapshot.ExecutionState.Status); err != nil {
		return err
	}

	expectedDBRecordVersion := setSnapshot.DBRecordVersion - 1

	transaction.AssertShard(false, request.RangeID)
	transaction.AssertWorkflowExecution(
		true,
		namespaceID,
		setSnapshot.WorkflowID,
		runID,
		&expectedDBRecordVersion,
		false,
	)

	transaction.HandleWorkflowSnapshot(&setSnapshot)
	transaction.DeleteStateItems(namespaceID, setSnapshot.WorkflowID, runID)
	return transaction.Execute(ctx)
}

func (d *BaseMutableStateStore) SetCurrentExecution(
	ctx context.Context,
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
	executionStateBlob *commonpb.DataBlob,
	lastWriteVersion int64,
	state enums.WorkflowExecutionState,
	tf executor.TransactionFactory,
) (err error) {
	transaction := tf.NewTransaction(shardID)
	transaction.UpsertCurrentWorkflow(
		primitives.MustParseUUID(namespaceID),
		workflowID,
		primitives.MustParseUUID(runID),
		executionStateBlob,
		lastWriteVersion,
		state,
	)
	return transaction.Execute(ctx)
}

func (d *BaseMutableStateStore) PutWorkflowExecution(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
	asCurrent bool,
	tf executor.TransactionFactory,
) (err error) {
	transaction := tf.NewTransaction(request.ShardID)
	return d.PutWorkflowExecutionWithinTransaction(ctx, request, asCurrent, transaction)
}

func (d *BaseMutableStateStore) PutWorkflowExecutionWithinTransaction(
	ctx context.Context,
	request *p.InternalSetWorkflowExecutionRequest,
	asCurrent bool,
	transaction executor.Transaction,
) (err error) {
	setSnapshot := request.SetWorkflowSnapshot
	namespaceID := primitives.MustParseUUID(setSnapshot.NamespaceID)
	runID := primitives.MustParseUUID(setSnapshot.RunID)

	if err = p.ValidateUpdateWorkflowStateStatus(setSnapshot.ExecutionState.State, setSnapshot.ExecutionState.Status); err != nil {
		return err
	}

	expectedDBRecordVersion := setSnapshot.DBRecordVersion

	transaction.AssertShard(false, request.RangeID)
	transaction.AssertWorkflowExecution(
		true,
		namespaceID,
		setSnapshot.WorkflowID,
		runID,
		&expectedDBRecordVersion,
		false,
	)
	if asCurrent {
		transaction.AssertCurrentWorkflow(
			true,
			namespaceID,
			setSnapshot.WorkflowID,
			nil,
			primitives.MustParseUUID(request.SetWorkflowSnapshot.RunID),
			false,
			nil,
		)
		transaction.UpsertCurrentWorkflow(
			primitives.MustParseUUID(request.SetWorkflowSnapshot.NamespaceID),
			request.SetWorkflowSnapshot.WorkflowID,
			primitives.MustParseUUID(request.SetWorkflowSnapshot.RunID),
			request.SetWorkflowSnapshot.ExecutionStateBlob,
			request.SetWorkflowSnapshot.LastWriteVersion,
			request.SetWorkflowSnapshot.ExecutionState.State,
		)
	}

	transaction.HandleWorkflowSnapshot(&setSnapshot)
	transaction.DeleteStateItems(namespaceID, setSnapshot.WorkflowID, runID)
	return transaction.Execute(ctx)
}
