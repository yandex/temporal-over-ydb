package persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	ShardStore struct {
		clusterName string
		client      *xydb.Client
		logger      log.Logger
	}
)

func NewShardStore(
	clusterName string,
	client *xydb.Client,
	logger log.Logger,
) *ShardStore {
	return &ShardStore{
		clusterName: clusterName,
		client:      client,
		logger:      logger,
	}
}

func (d *ShardStore) GetOrCreateShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (resp *p.InternalGetOrCreateShardResponse, err error) {
	shardID := request.ShardID

	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v", shardID)
			err = xydb.ConvertToTemporalError("GetOrCreateShard", err, details)
		}
	}()

	shardInfo, err := d.getShard(ctx, shardID)
	if err != nil {
		return nil, err
	}
	if shardInfo != nil {
		resp = &p.InternalGetOrCreateShardResponse{
			ShardInfo: shardInfo,
		}
		return resp, nil
	}
	if request.CreateShardInfo == nil {
		return nil, errors.New("shard not found and CreateShardInfo is nil")
	}

	// shard was not found and we should create it
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, err
	}
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $range_id AS int64;
DECLARE $shard AS string;
DECLARE $shard_encoding AS utf8;

UPSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_id, task_category_id, task_visibility_ts, event_type, event_id, event_name, range_id, shard, shard_encoding)
VALUES ($shard_id, "", "", "", NULL, NULL, NULL, NULL, NULL, NULL, $range_id, $shard, $shard_encoding);
`)
	err = d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$shard", types.BytesValue(shardInfo.Data)),
		table.ValueParam("$shard_encoding", types.UTF8Value(shardInfo.EncodingType.String())),
		table.ValueParam("$range_id", types.Int64Value(rangeID)),
	))

	if ydb.IsOperationErrorAlreadyExistsError(err) {
		shardInfo, err = d.getShard(ctx, shardID)
		if err != nil {
			return nil, err
		}
		if shardInfo == nil {
			return nil, errors.New("couldn't get shard that already exists")
		}
	} else if err != nil {
		return
	}

	resp = &p.InternalGetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}
	return resp, nil
}

func (d *ShardStore) UpdateShard(ctx context.Context, request *p.InternalUpdateShardRequest) (err error) {
	defer func() {
		if err != nil {
			details := fmt.Sprintf("shard_id: %v, previous_range_id: %v", request.ShardID, request.PreviousRangeID)
			err = xydb.ConvertToTemporalError("UpdateShard", err, details)
		}
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $prev_range_id AS int64;
DECLARE $range_id AS int64;
DECLARE $shard AS string;
DECLARE $shard_encoding AS utf8;

DISCARD SELECT Ensure(range_id, range_id == $prev_range_id, "RANGE_ID_MISMATCH")
FROM executions
WHERE shard_id = $shard_id
AND namespace_id = ""
AND workflow_id = ""
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;

UPSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_id, task_category_id, task_visibility_ts, event_type, event_id, event_name, range_id, shard, shard_encoding)
VALUES ($shard_id, "", "", "", NULL, NULL, NULL, NULL, NULL, NULL, $range_id, $shard, $shard_encoding);
`)
	err = d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$range_id", types.Int64Value(request.RangeID)),
		table.ValueParam("$shard", types.BytesValue(request.ShardInfo.Data)),
		table.ValueParam("$shard_encoding", types.UTF8Value(request.ShardInfo.EncodingType.String())),
		table.ValueParam("$prev_range_id", types.Int64Value(request.PreviousRangeID)),
	))
	if xydb.IsPreconditionFailedAndContains(err, "RANGE_ID_MISMATCH") {
		return xydb.WrapErrorAsRootCause(&p.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg:     fmt.Sprintf("failed to update shard (previous_range_id: %v)", request.PreviousRangeID),
		})
	}
	return err
}

func (d *ShardStore) AssertShardOwnership(ctx context.Context, request *p.AssertShardOwnershipRequest) error {
	return nil
}

func (d *ShardStore) GetName() string {
	return ydbPersistenceName
}

func (d *ShardStore) GetClusterName() string {
	return d.clusterName
}

func (d *ShardStore) Close() {
}

func (d *ShardStore) getShard(ctx context.Context, shardID int32) (rv *commonpb.DataBlob, err error) {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;

SELECT shard, shard_encoding
FROM executions
WHERE shard_id = $shard_id
AND namespace_id = ""
AND workflow_id = ""
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
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
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}
	if !res.NextRow() {
		return nil, nil
	}
	var data []byte
	var encoding string
	if err = res.ScanNamed(
		named.OptionalWithDefault("shard", &data),
		named.OptionalWithDefault("shard_encoding", &encoding),
	); err != nil {
		return nil, fmt.Errorf("failed to scan shard: %w", err)
	}
	return p.NewDataBlob(data, encoding), nil
}
