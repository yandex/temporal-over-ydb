package ydb

import (
	"context"
	"errors"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/rows"
)

type (
	ShardStore struct {
		clusterName string
		client      *conn.Client
		logger      log.Logger
		tf          executor.TransactionFactory
	}
)

func NewShardStore(
	clusterName string,
	client *conn.Client,
	logger log.Logger,
) *ShardStore {
	tf := rows.NewTransactionFactory(client)
	return &ShardStore{
		clusterName: clusterName,
		client:      client,
		logger:      logger,
		tf:          tf,
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
			err = conn.ConvertToTemporalError("GetOrCreateShard", err, details)
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
DECLARE $shard_encoding AS ` + d.client.EncodingType().String() + `;

INSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_id, task_category_id, task_visibility_ts, event_type, event_id, event_name, range_id, shard, shard_encoding)
VALUES ($shard_id, "", "", "", NULL, NULL, NULL, NULL, NULL, NULL, $range_id, $shard, $shard_encoding);
`)
	err = d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$shard", types.BytesValue(shardInfo.Data)),
		table.ValueParam("$shard_encoding", d.client.EncodingTypeValue(shardInfo.EncodingType)),
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
			err = conn.ConvertToTemporalError("UpdateShard", err)
		}
	}()

	transaction := d.tf.NewTransaction(request.ShardID)
	transaction.AssertShard(true, request.PreviousRangeID)
	transaction.UpsertShard(request.RangeID, request.ShardInfo)
	return transaction.Execute(ctx)
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
	res, err := d.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
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
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if d.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("shard_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("shard_encoding", &encoding)
	}
	if err = res.ScanNamed(
		named.OptionalWithDefault("shard", &data),
		encodingScanner,
	); err != nil {
		return nil, fmt.Errorf("failed to scan shard: %w", err)
	}
	if d.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}
	return p.NewDataBlob(data, encoding), nil
}
