package persistencelite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"

	"github.com/pborman/uuid"
	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/cas"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	errorslite "github.com/yandex/temporal-over-ydb/persistencelite/errors"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	ShardStore struct {
		clusterName   string
		rqliteFactory conn.RqliteFactory
		logger        log.Logger
	}
)

func NewShardStore(
	clusterName string,
	rqliteFactory conn.RqliteFactory,
	logger log.Logger,
) *ShardStore {
	return &ShardStore{
		clusterName:   clusterName,
		rqliteFactory: rqliteFactory,
		logger:        logger,
	}
}

const (
	createShardQry = `
INSERT INTO shards (shard_id, range_id, data, data_encoding)
VALUES (?, ?, ?, ?)`

	getShardQry = `
SELECT shard_id, range_id, data, data_encoding
FROM shards 
WHERE shard_id = ?`

	updateShardQry = `
UPDATE shards
SET range_id = ?, data = ?, data_encoding = ?
WHERE shard_id = ? AND (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0;
`
)

// SelectFromShards reads one or more rows from shards table
func (d *ShardStore) selectFromShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (*sqlplugin.ShardsRow, error) {
	rql := d.rqliteFactory.GetClient(filter.ShardID)
	qr, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     getShardQry,
			Arguments: []interface{}{filter.ShardID},
		},
	})
	if err != nil {
		return nil, err
	}
	res := qr[0]
	if !res.Next() {
		return nil, sql.ErrNoRows
	}
	var (
		shardID      int64
		rangeID      int64
		dataB64      string
		dataEncoding string
	)
	if err = res.Scan(&shardID, &rangeID, &dataB64, &dataEncoding); err != nil {
		return nil, fmt.Errorf("failed to scan shard: %w", err)
	}
	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode shard data: %w", err)
	}
	return &sqlplugin.ShardsRow{
		ShardID:      int32(shardID),
		RangeID:      rangeID,
		Data:         data,
		DataEncoding: dataEncoding,
	}, nil
}

func (d *ShardStore) GetOrCreateShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (resp *p.InternalGetOrCreateShardResponse, err error) {
	row, err := d.selectFromShards(ctx, sqlplugin.ShardsFilter{
		ShardID: request.ShardID,
	})
	switch err {
	case nil:
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: p.NewDataBlob(row.Data, row.DataEncoding),
		}, nil
	case sql.ErrNoRows:
	default:
		return nil, errorslite.NewUnavailableF("GetOrCreateShard: failed to get ShardID %v. Error: %v", request.ShardID, err)
	}

	if request.CreateShardInfo == nil {
		return nil, errorslite.NewUnavailableF("GetOrCreateShard: ShardID %v not found. Error: %v", request.ShardID, err)
	}

	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, errorslite.NewUnavailableF("GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v", request.ShardID, err)
	}
	row = &sqlplugin.ShardsRow{
		ShardID:      request.ShardID,
		RangeID:      rangeID,
		Data:         shardInfo.Data,
		DataEncoding: shardInfo.EncodingType.String(),
	}
	rql := d.rqliteFactory.GetClient(row.ShardID)
	_, err = rql.WriteParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     createShardQry,
			Arguments: []interface{}{row.ShardID, row.RangeID, row.Data, row.DataEncoding},
		},
	})
	if err == nil {
		return &p.InternalGetOrCreateShardResponse{ShardInfo: shardInfo}, nil
	} else {
		return nil, errorslite.NewUnavailableF("GetOrCreateShard: failed to insert into shards table. Error: %v", err)
	}
}

func (d *ShardStore) UpdateShard(ctx context.Context, request *p.InternalUpdateShardRequest) (err error) {
	shardID := request.ShardID
	condID := uuid.New()
	assertions := []cas.Assertion{
		&cas.ShardRangeCond{
			ShardID:        shardID,
			RangeIDEqualTo: request.PreviousRangeID,
		},
	}

	stmts := []gorqlite.ParameterizedStatement{
		{
			Query:     updateShardQry,
			Arguments: []interface{}{request.RangeID, request.ShardInfo.Data, request.ShardInfo.EncodingType.String(), shardID, condID},
		},
	}

	rql := d.rqliteFactory.GetClient(shardID)
	return cas.ExecuteConditionally(ctx, rql, shardID, "", condID, assertions, nil, nil, stmts)
}

func (d *ShardStore) AssertShardOwnership(ctx context.Context, request *p.AssertShardOwnershipRequest) error {
	return nil
}

func (d *ShardStore) GetName() string {
	return persistenceName
}

func (d *ShardStore) GetClusterName() string {
	return d.clusterName
}

func (d *ShardStore) Close() {
}
