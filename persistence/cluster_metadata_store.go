package persistence

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	ClusterMetadataStore struct {
		client *xydb.Client
		logger log.Logger
	}
)

var _ p.ClusterMetadataStore = (*ClusterMetadataStore)(nil)

// NewClusterMetadataStore is used to create an instance of ClusterMetadataStore implementation
func NewClusterMetadataStore(
	client *xydb.Client,
	logger log.Logger,
) (p.ClusterMetadataStore, error) {
	return &ClusterMetadataStore{
		client: client,
		logger: logger,
	}, nil
}

func (m *ClusterMetadataStore) ListClusterMetadata(
	ctx context.Context,
	request *p.InternalListClusterMetadataRequest,
) (resp *p.InternalListClusterMetadataResponse, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("ListClusterMetadata", err)
		}
	}()

	var pageToken clusterMetadataPageToken
	if err = pageToken.deserialize(request.NextPageToken); err != nil {
		return nil, err
	}

	params := table.NewQueryParameters(
		table.ValueParam("$cluster_name", types.UTF8Value(pageToken.ClusterName)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	)
	template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $page_size AS int32;

SELECT data, data_encoding, version, cluster_name
FROM cluster_metadata_info
WHERE cluster_name > $cluster_name
ORDER BY cluster_name
LIMIT $page_size;
`)

	res, err := m.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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
		return
	}

	resp = &p.InternalListClusterMetadataResponse{}
	var nextPageToken clusterMetadataPageToken
	for res.NextRow() {
		blob, clusterName, version, err := scanClusterMetadata(res)
		if err != nil {
			return nil, err
		}
		nextPageToken.ClusterName = clusterName
		resp.ClusterMetadata = append(resp.ClusterMetadata, &p.InternalGetClusterMetadataResponse{
			ClusterMetadata: blob,
			Version:         version,
		})
	}
	if len(resp.ClusterMetadata) >= request.PageSize {
		if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (m *ClusterMetadataStore) GetClusterMetadata(
	ctx context.Context,
	request *p.InternalGetClusterMetadataRequest,
) (resp *p.InternalGetClusterMetadataResponse, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("GetClusterMetadata", err)
		}
	}()
	template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;

SELECT cluster_name, data, data_encoding, version
FROM cluster_metadata_info
WHERE cluster_name = $cluster_name;
`)

	res, err := m.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
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
	if err = xydb.EnsureOneRowCursor(ctx, res); err != nil {
		return
	}
	blob, _, version, err := scanClusterMetadata(res)
	if err != nil {
		return
	}

	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: blob,
		Version:         version,
	}, nil
}

func scanClusterMetadata(res result.Result) (*commonpb.DataBlob, string, int64, error) {
	var clusterName string
	var data []byte
	var encoding string
	var version int64
	if err := res.ScanNamed(
		named.OptionalWithDefault("cluster_name", &clusterName),
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", &encoding),
		named.OptionalWithDefault("version", &version),
	); err != nil {
		return nil, "", 0, fmt.Errorf("failed to scan cluster metadata: %w", err)
	}
	return p.NewDataBlob(data, encoding), clusterName, version, nil
}

func (m *ClusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *p.InternalSaveClusterMetadataRequest,
) (rv bool, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("SaveClusterMetadata", err)
		}
	}()
	if request.Version == 0 {
		template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $data AS string;
DECLARE $encoding AS utf8;
DECLARE $version AS int64;

INSERT INTO cluster_metadata_info (cluster_name, data, data_encoding, version)
VALUES ($cluster_name, $data, $encoding, $version);
`)
		if err = m.client.Write(ctx, template, table.NewQueryParameters(
			table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
			table.ValueParam("$data", types.BytesValue(request.ClusterMetadata.Data)),
			table.ValueParam("$encoding", types.UTF8Value(request.ClusterMetadata.EncodingType.String())),
			table.ValueParam("$version", types.Int64Value(1)),
		)); err != nil {
			return false, err
		}
	} else {
		template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $data AS string;
DECLARE $encoding AS utf8;
DECLARE $version AS int64;
DECLARE $prev_version AS int64;

DISCARD SELECT Ensure(version, version == $prev_version, "VERSION_MISMATCH")
FROM cluster_metadata_info
WHERE cluster_name = $cluster_name;

UPDATE cluster_metadata_info
SET
data = $data,
data_encoding = $encoding,
version = $version
WHERE cluster_name = $cluster_name;
`)
		if err = m.client.Write(ctx, template, table.NewQueryParameters(
			table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
			table.ValueParam("$data", types.BytesValue(request.ClusterMetadata.Data)),
			table.ValueParam("$encoding", types.UTF8Value(request.ClusterMetadata.EncodingType.String())),
			table.ValueParam("$version", types.Int64Value(request.Version+1)),
			table.ValueParam("$prev_version", types.Int64Value(request.Version)),
		)); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (m *ClusterMetadataStore) DeleteClusterMetadata(
	ctx context.Context,
	request *p.InternalDeleteClusterMetadataRequest,
) error {
	template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;

DELETE FROM cluster_metadata_info
WHERE cluster_name = $cluster_name;
`)
	err := m.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
	))
	return xydb.ConvertToTemporalError("DeleteClusterMetadata", err)
}

func (m *ClusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *p.GetClusterMembersRequest,
) (resp *p.GetClusterMembersResponse, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("GetClusterMembers", err)
		}
	}()

	var lastSeenHostID uuid.UUID
	if len(request.NextPageToken) > 0 {
		if lastSeenHostID, err = uuid.ParseBytes(request.NextPageToken); err != nil {
			return nil, serviceerror.NewInternal("page token is corrupted.")
		}
	}

	params := table.NewQueryParameters(
		table.ValueParam("$expire_at_gt", types.TimestampValueFromTime(ToYDBDateTime(time.Now()))),
	)
	var declares []string
	var suffixes []string
	if request.HostIDEquals != nil {
		declares = append(declares, "DECLARE $host_id AS utf8;")
		params.Add(table.ValueParam("$host_id", types.UTF8Value(request.HostIDEquals.String())))
		suffixes = append(suffixes, "AND host_id = $host_id")
	}
	if request.RPCAddressEquals != nil {
		declares = append(declares, "DECLARE $rpc_address AS utf8;")
		params.Add(table.ValueParam("$rpc_address", types.UTF8Value(request.RPCAddressEquals.String())))
		suffixes = append(suffixes, "AND rpc_address = $rpc_address")
	}
	if request.RoleEquals != p.All {
		declares = append(declares, "DECLARE $role AS int32;")
		params.Add(table.ValueParam("$role", types.Int32Value(int32(request.RoleEquals))))
		suffixes = append(suffixes, "AND role = $role")
	}
	if !request.SessionStartedAfter.IsZero() {
		declares = append(declares, "DECLARE $session_started_gt AS Timestamp;")
		params.Add(table.ValueParam("$session_started_gt", types.TimestampValueFromTime(ToYDBDateTime(request.SessionStartedAfter))))
		suffixes = append(suffixes, "AND session_start > $session_started_gt")
	}
	if request.LastHeartbeatWithin > 0 {
		declares = append(declares, "DECLARE $last_heartbeat_gt AS Timestamp;")
		params.Add(table.ValueParam("$last_heartbeat_gt", types.TimestampValueFromTime(ToYDBDateTime(time.Now().Add(-request.LastHeartbeatWithin)))))
		suffixes = append(suffixes, "AND last_heartbeat > $last_heartbeat_gt")
	}
	if lastSeenHostID != nil && request.HostIDEquals == nil {
		declares = append(declares, "DECLARE $host_id_gt AS utf8;")
		params.Add(table.ValueParam("$host_id_gt", types.UTF8Value(lastSeenHostID.String())))
		suffixes = append(suffixes, "AND host_id > $host_id_gt")
	}
	if request.PageSize > 0 {
		declares = append(declares, "DECLARE $page_size AS int32;")
		params.Add(table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))))
	}

	template := m.client.AddQueryPrefix(`
DECLARE $expire_at_gt AS Timestamp;
` + strings.Join(declares, "\n") + `
SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, expire_at
FROM cluster_membership
WHERE expire_at > $expire_at_gt ` + strings.Join(suffixes, " ") + `
ORDER BY host_id
`)
	if request.PageSize > 0 {
		template += " LIMIT $page_size;"
	}
	res, err := m.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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

	members := make([]*p.ClusterMember, 0, request.PageSize)
	for res.NextRow() {
		member, err := scanClusterMember(res)
		if err != nil {
			return nil, err
		}
		members = append(members, member)
	}

	var nextPageToken []byte
	if request.PageSize > 0 && len(members) == request.PageSize {
		nextPageToken, err = members[len(members)-1].HostID.MarshalText()
		if err != nil {
			return nil, fmt.Errorf("failed to create next page token")
		}
	}
	return &p.GetClusterMembersResponse{
		ActiveMembers: members,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *ClusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *p.UpsertClusterMembershipRequest,
) error {
	template := m.client.AddQueryPrefix(`
DECLARE $host_id AS utf8;
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
			table.ValueParam("$host_id", types.UTF8Value(request.HostID.String())),
			table.ValueParam("$rpc_address", types.UTF8Value(request.RPCAddress.String())),
			table.ValueParam("$rpc_port", types.Int32Value(int32(request.RPCPort))),
			table.ValueParam("$role", types.Int32Value(int32(request.Role))),
			table.ValueParam("$session_start", types.TimestampValueFromTime(ToYDBDateTime(request.SessionStart))),
			table.ValueParam("$last_heartbeat", types.TimestampValueFromTime(ToYDBDateTime(now))),
			table.ValueParam("$expire_at", types.TimestampValueFromTime(ToYDBDateTime(now.Add(request.RecordExpiry)))),
		)
	})
	return xydb.ConvertToTemporalError("UpsertClusterMembership", err)
}

func (m *ClusterMetadataStore) PruneClusterMembership(
	ctx context.Context,
	request *p.PruneClusterMembershipRequest,
) error {
	template := m.client.AddQueryPrefix(`
DECLARE $expire_at_lt AS Timestamp;

DELETE FROM cluster_membership
WHERE expire_at < $expire_at_lt;
`)
	err := m.client.Write2(ctx, template, func() *table.QueryParameters {
		return table.NewQueryParameters(
			table.ValueParam("$expire_at_lt", types.TimestampValueFromTime(ToYDBDateTime(time.Now()))),
		)
	})
	return xydb.ConvertToTemporalError("PruneClusterMembership", err)
}

func (m *ClusterMetadataStore) GetName() string {
	return ydbPersistenceName
}

func (m *ClusterMetadataStore) Close() {
}

func scanClusterMember(res result.Result) (*p.ClusterMember, error) {
	var hostID, rpcAddress string
	var rpcPort, role int32
	var sessionStart, lastHeartbeat, expireAt time.Time

	if err := res.ScanNamed(
		named.OptionalWithDefault("host_id", &hostID),
		named.OptionalWithDefault("rpc_address", &rpcAddress),
		named.OptionalWithDefault("rpc_port", &rpcPort),
		named.OptionalWithDefault("role", &role),
		named.OptionalWithDefault("session_start", &sessionStart),
		named.OptionalWithDefault("last_heartbeat", &lastHeartbeat),
		named.OptionalWithDefault("expire_at", &expireAt),
	); err != nil {
		return nil, fmt.Errorf("failed to scan cluster member: %w", err)
	}

	return &p.ClusterMember{
		HostID:        uuid.Parse(hostID),
		RPCAddress:    net.ParseIP(rpcAddress),
		RPCPort:       uint16(rpcPort),
		Role:          p.ServiceType(role),
		SessionStart:  FromYDBDateTime(sessionStart),
		LastHeartbeat: FromYDBDateTime(lastHeartbeat),
		RecordExpiry:  FromYDBDateTime(expireAt),
	}, nil
}
