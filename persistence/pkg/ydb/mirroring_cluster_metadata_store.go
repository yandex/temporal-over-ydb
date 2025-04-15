package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	p "go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	MirroringClusterMetadataStore struct {
		client *conn.Client
	}
)

// NewMirroringClusterMetadataStore is used to create an instance of MirroringClusterMetadataStore implementation
func NewMirroringClusterMetadataStore(
	client *conn.Client,
) (*MirroringClusterMetadataStore, error) {
	return &MirroringClusterMetadataStore{
		client: client,
	}, nil
}

func (m *MirroringClusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *p.InternalSaveClusterMetadataRequest,
) (rv bool, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("SaveClusterMetadata", err)
		}
	}()

	template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $data AS string;
DECLARE $data_encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $version AS int64;

UPSERT INTO cluster_metadata_info (cluster_name, data, data_encoding, version)
VALUES ($cluster_name, $data, $data_encoding, $version);
`)
	if err = m.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
		table.ValueParam("$data", types.BytesValue(request.ClusterMetadata.Data)),
		table.ValueParam("$data_encoding", m.client.EncodingTypeValue(request.ClusterMetadata.EncodingType)),
		table.ValueParam("$version", types.Int64Value(request.Version)),
	)); err != nil {
		return false, err
	}
	return true, nil
}

func (m *MirroringClusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *p.UpsertClusterMembershipRequest,
) error {
	template := m.client.AddQueryPrefix(m.client.HostIDDecl() + `
DECLARE $rpc_address AS utf8;
DECLARE $rpc_port AS int32;
DECLARE $role AS int32;
DECLARE $session_start AS Timestamp;
DECLARE $last_heartbeat AS Timestamp;
DECLARE $expire_at AS Timestamp;

UPSERT INTO cluster_membership (host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, expire_at)
VALUES ($host_id, $rpc_address, $rpc_port, $role, $session_start, $last_heartbeat, $expire_at);
`)
	err := m.client.Write2(ctx, template, func() *table.QueryParameters {
		now := time.Now()
		return table.NewQueryParameters(
			table.ValueParam("$host_id", m.client.HostIDValueFromUUID(request.HostID)),
			table.ValueParam("$rpc_address", types.UTF8Value(request.RPCAddress.String())),
			table.ValueParam("$rpc_port", types.Int32Value(int32(request.RPCPort))),
			table.ValueParam("$role", types.Int32Value(int32(request.Role))),
			table.ValueParam("$session_start", types.TimestampValueFromTime(conn.ToYDBDateTime(request.SessionStart))),
			table.ValueParam("$last_heartbeat", types.TimestampValueFromTime(conn.ToYDBDateTime(now))),
			table.ValueParam("$expire_at", types.TimestampValueFromTime(conn.ToYDBDateTime(now.Add(request.RecordExpiry)))),
		)
	})
	return conn.ConvertToTemporalError("UpsertClusterMembership", err)
}
