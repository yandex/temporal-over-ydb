package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/mss"
	baserows "github.com/yandex/temporal-over-ydb/persistence/pkg/base/rows"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/rows"
)

type (
	MutableStateStore struct {
		client    *conn.Client
		logger    log.Logger
		baseStore *mss.BaseMutableStateStore
		tf        executor.TransactionFactory
	}
)

func NewMutableStateStore(
	client *conn.Client,
	logger log.Logger,
	cache executor.EventsCache,
) *MutableStateStore {
	tf := rows.NewTransactionFactory(client)
	return &MutableStateStore{
		client:    client,
		logger:    logger,
		baseStore: mss.NewBaseMutableStateStore(cache),
		tf:        tf,
	}
}

func (d *MutableStateStore) createWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	return d.baseStore.CreateWorkflowExecution(ctx, request, d.tf)
}

func (d *MutableStateStore) updateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (err error) {
	return d.baseStore.UpdateWorkflowExecution(ctx, request, d.tf)
}

func (d *MutableStateStore) conflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (err error) {
	return d.baseStore.ConflictResolveWorkflowExecution(ctx, request, d.tf)
}

func (d *MutableStateStore) SetWorkflowExecution(ctx context.Context, request *p.InternalSetWorkflowExecutionRequest) (err error) {
	err = d.baseStore.SetWorkflowExecution(ctx, request, d.tf)
	if err != nil {
		err = conn.ConvertToTemporalError("SetWorkflowExecution", err)
	}
	return
}

func (d *MutableStateStore) GetWorkflowExecution(
	ctx context.Context,
	request *p.GetWorkflowExecutionRequest,
) (resp *p.InternalGetWorkflowExecutionResponse, err error) {
	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, namespace_id: %v, workflow_id: %v, run_id: %v",
				request.ShardID, request.NamespaceID, request.WorkflowID, request.RunID)
			err = conn.ConvertToTemporalError("GetWorkflowExecution", err, details)
		}
	}()

	// With a single SELECT we read the execution row and all its events
	template := d.client.AddQueryPrefix(d.client.NamspaceIDDecl() + d.client.RunIDDecl() + `
DECLARE $shard_id AS uint32;
DECLARE $workflow_id AS Utf8;

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
	res, err := d.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", d.client.NamespaceIDValue(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", d.client.RunIDValue(request.RunID)),
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
		return nil, conn.NewRootCauseError(serviceerror.NewNotFound, "workflow execution not found")
	}

	state, dbRecordVersion, err := d.scanMutableState(res)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, conn.NewRootCauseError(serviceerror.NewNotFound, "workflow execution not found")
	}

	state.ActivityInfos = make(map[int64]*commonpb.DataBlob)
	state.TimerInfos = make(map[string]*commonpb.DataBlob)
	state.ChildExecutionInfos = make(map[int64]*commonpb.DataBlob)
	state.RequestCancelInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalInfos = make(map[int64]*commonpb.DataBlob)
	state.SignalRequestedIDs = make([]string, 0)
	state.BufferedEvents = make([]*commonpb.DataBlob, 0)

	for res.NextRow() {
		var itemID int64
		var itemName string
		var itemType int32
		var data []byte
		var encoding string
		var encodingType conn.EncodingTypeRaw
		var encodingScanner named.Value
		if d.client.UseIntForEncoding() {
			encodingScanner = named.OptionalWithDefault("data_encoding", &encodingType)
		} else {
			encodingScanner = named.OptionalWithDefault("data_encoding", &encoding)
		}
		if err = res.ScanNamed(
			named.OptionalWithDefault("event_type", &itemType),
			named.OptionalWithDefault("event_id", &itemID),
			named.OptionalWithDefault("event_name", &itemName),
			named.OptionalWithDefault("data", &data),
			encodingScanner,
		); err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		if d.client.UseIntForEncoding() {
			encoding = enumspb.EncodingType(encodingType).String()
		}
		switch itemType {
		case baserows.ItemTypeActivity:
			state.ActivityInfos[itemID] = p.NewDataBlob(data, encoding)
		case baserows.ItemTypeTimer:
			state.TimerInfos[itemName] = p.NewDataBlob(data, encoding)
		case baserows.ItemTypeChildExecution:
			state.ChildExecutionInfos[itemID] = p.NewDataBlob(data, encoding)
		case baserows.ItemTypeRequestCancel:
			state.RequestCancelInfos[itemID] = p.NewDataBlob(data, encoding)
		case baserows.ItemTypeSignal:
			state.SignalInfos[itemID] = p.NewDataBlob(data, encoding)
		case baserows.ItemTypeSignalRequested:
			state.SignalRequestedIDs = append(state.SignalRequestedIDs, itemName)
		case baserows.ItemTypeBufferedEvent:
			state.BufferedEvents = append(state.BufferedEvents, p.NewDataBlob(data, encoding))
		default:
			return nil, fmt.Errorf("unknown item type: %d", itemType)
		}
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbRecordVersion,
	}, nil
}

func (d *MutableStateStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *p.DeleteWorkflowExecutionRequest,
) error {
	// XXX TODO delete events as well?
	template := d.client.AddQueryPrefix(d.client.NamspaceIDDecl() + d.client.RunIDDecl() + `
DECLARE $shard_id AS uint32;
DECLARE $workflow_id AS Utf8;

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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", d.client.NamespaceIDValue(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", d.client.RunIDValue(request.RunID)),
	))

	if err != nil {
		return conn.ConvertToTemporalError("DeleteWorkflowExecution", err)
	}
	return nil
}

func (d *MutableStateStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *p.DeleteCurrentWorkflowExecutionRequest,
) error {
	template := d.client.AddQueryPrefix(d.client.NamspaceIDDecl() + d.client.RunIDDecl() + d.client.CurrentRunIDDecl() + `
DECLARE $shard_id AS uint32;
DECLARE $workflow_id AS Utf8;

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
AND event_name IS NULL
AND current_run_id = $current_run_id;
	`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", d.client.NamespaceIDValue(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", d.client.EmptyRunIDValue()),
		table.ValueParam("$current_run_id", d.client.RunIDValue(request.RunID)),
	))
	if err != nil {
		return conn.ConvertToTemporalError("DeleteCurrentWorkflowExecution", err)
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
			err = conn.ConvertToTemporalError("GetCurrentExecution", err, details)
		}
	}()
	query := d.client.AddQueryPrefix(d.client.NamspaceIDDecl() + d.client.RunIDDecl() + `
DECLARE $shard_id AS uint32;
DECLARE $workflow_id AS Utf8;

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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$namespace_id", d.client.NamespaceIDValue(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", d.client.EmptyRunIDValue()),
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

	if err = conn.EnsureOneRowCursor(ctx, res); err != nil {
		return nil, err
	}
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingPtr interface{}
	if d.client.UseIntForEncoding() {
		encodingPtr = &encodingType
	} else {
		encodingPtr = &encoding
	}

	var currentRunID string
	var currentRunIDBytes []byte
	var currentRunIDPtr interface{}
	if d.client.UseBytesForRunIDs() {
		currentRunIDPtr = &currentRunIDBytes
	} else {
		currentRunIDPtr = &currentRunID
	}

	if err = res.ScanWithDefaults(currentRunIDPtr, &data, encodingPtr); err != nil {
		return nil, fmt.Errorf("failed to scan current workflow execution row: %w", err)
	}
	if d.client.UseBytesForRunIDs() {
		currentRunID = primitives.UUIDString(currentRunIDBytes)
	}
	if d.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}
	blob := p.NewDataBlob(data, encoding)
	executionState, err := serialization.WorkflowExecutionStateFromBlob(blob.Data, blob.EncodingType.String())
	if err != nil {
		return nil, err
	}

	return &p.InternalGetCurrentExecutionResponse{
		RunID:          currentRunID,
		ExecutionState: executionState,
	}, nil
}

func (d *MutableStateStore) ListConcreteExecutions(
	_ context.Context,
	_ *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListConcreteExecutions is not implemented")
}

func (d *MutableStateStore) scanMutableState(res result.Result) (*p.InternalWorkflowMutableState, int64, error) {
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingPtr interface{}
	if d.client.UseIntForEncoding() {
		encodingPtr = &encodingType
	} else {
		encodingPtr = &encoding
	}
	var nextEventID int64
	var dbRecordVersion int64
	var stateData []byte
	var stateEncoding string
	var stateEncodingType conn.EncodingTypeRaw
	var stateEncodingPtr interface{}
	if d.client.UseIntForEncoding() {
		stateEncodingPtr = &stateEncodingType
	} else {
		stateEncodingPtr = &stateEncoding
	}
	var checksumData []byte
	var checksumEncoding string
	var checksumEncodingType conn.EncodingTypeRaw
	var checksumEncodingPtr interface{}
	if d.client.UseIntForEncoding() {
		checksumEncodingPtr = &checksumEncodingType
	} else {
		checksumEncodingPtr = &checksumEncoding
	}
	var eventID int64
	var eventName string
	if err := res.ScanNamed(
		named.OptionalWithDefault("next_event_id", &nextEventID),
		named.OptionalWithDefault("execution", &data),
		named.OptionalWithDefault("execution_encoding", encodingPtr),
		named.OptionalWithDefault("db_record_version", &dbRecordVersion),
		named.OptionalWithDefault("execution_state", &stateData),
		named.OptionalWithDefault("execution_state_encoding", stateEncodingPtr),
		named.OptionalWithDefault("checksum", &checksumData),
		named.OptionalWithDefault("checksum_encoding", checksumEncodingPtr),
		named.OptionalWithDefault("event_id", &eventID),
		named.OptionalWithDefault("event_name", &eventName),
	); err != nil {
		return nil, 0, fmt.Errorf("failed to scan execution: %w", err)
	}
	if eventID > 0 || len(eventName) > 0 {
		// expected an execution, scanned an event
		return nil, 0, nil
	}
	if d.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
		stateEncoding = enumspb.EncodingType(stateEncodingType).String()
		checksumEncoding = enumspb.EncodingType(checksumEncodingType).String()
	}
	return &p.InternalWorkflowMutableState{
		ExecutionInfo:  p.NewDataBlob(data, encoding),
		ExecutionState: p.NewDataBlob(stateData, stateEncoding),
		Checksum:       p.NewDataBlob(checksumData, checksumEncoding),
		NextEventID:    nextEventID,
	}, dbRecordVersion, nil
}
