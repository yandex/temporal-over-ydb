package ydb

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	ClusterMetadataStore struct {
		client *conn.Client
		logger log.Logger
	}
)

var _ p.ClusterMetadataStore = (*ClusterMetadataStore)(nil)

// NewClusterMetadataStore is used to create an instance of ClusterMetadataStore implementation
func NewClusterMetadataStore(
	client *conn.Client,
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
			err = conn.ConvertToTemporalError("ListClusterMetadata", err)
		}
	}()

	var pageToken tokens.ClusterMetadataPageToken
	if err = pageToken.Deserialize(request.NextPageToken); err != nil {
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

	res, err := m.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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
	var nextPageToken tokens.ClusterMetadataPageToken
	for res.NextRow() {
		blob, clusterName, version, err := m.scanClusterMetadata(res)
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
		if resp.NextPageToken, err = nextPageToken.Serialize(); err != nil {
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
			err = conn.ConvertToTemporalError("GetClusterMetadata", err)
		}
	}()
	template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;

SELECT cluster_name, data, data_encoding, version
FROM cluster_metadata_info
WHERE cluster_name = $cluster_name;
`)

	res, err := m.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
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
	if err = conn.EnsureOneRowCursor(ctx, res); err != nil {
		return
	}
	blob, _, version, err := m.scanClusterMetadata(res)
	if err != nil {
		return
	}

	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: blob,
		Version:         version,
	}, nil
}

func (m *ClusterMetadataStore) scanClusterMetadata(res result.Result) (*commonpb.DataBlob, string, int64, error) {
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if m.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("data_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("data_encoding", &encoding)
	}

	var clusterName string
	var data []byte
	var version int64
	if err := res.ScanNamed(
		named.OptionalWithDefault("cluster_name", &clusterName),
		named.OptionalWithDefault("data", &data),
		encodingScanner,
		named.OptionalWithDefault("version", &version),
	); err != nil {
		return nil, "", 0, fmt.Errorf("failed to scan cluster metadata: %w", err)
	}

	if m.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}
	return p.NewDataBlob(data, encoding), clusterName, version, nil
}

func (m *ClusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *p.InternalSaveClusterMetadataRequest,
) (rv bool, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("SaveClusterMetadata", err)
		}
	}()
	if request.Version == 0 {
		template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $data AS string;
DECLARE $encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $version AS int64;

INSERT INTO cluster_metadata_info (cluster_name, data, data_encoding, version)
VALUES ($cluster_name, $data, $encoding, $version);
`)
		if err = m.client.Write(ctx, template, table.NewQueryParameters(
			table.ValueParam("$cluster_name", types.UTF8Value(request.ClusterName)),
			table.ValueParam("$data", types.BytesValue(request.ClusterMetadata.Data)),
			table.ValueParam("$encoding", m.client.EncodingTypeValue(request.ClusterMetadata.EncodingType)),
			table.ValueParam("$version", types.Int64Value(1)),
		)); err != nil {
			return false, err
		}
	} else {
		template := m.client.AddQueryPrefix(`
DECLARE $cluster_name AS utf8;
DECLARE $data AS string;
DECLARE $encoding AS ` + m.client.EncodingType().String() + `;
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
			table.ValueParam("$encoding", m.client.EncodingTypeValue(request.ClusterMetadata.EncodingType)),
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
	return conn.ConvertToTemporalError("DeleteClusterMetadata", err)
}

func (m *ClusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *p.GetClusterMembersRequest,
) (resp *p.GetClusterMembersResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("GetClusterMembers", err)
		}
	}()

	var pageToken tokens.ClusterMembersPageToken
	if len(request.NextPageToken) > 0 {
		if err = pageToken.Deserialize(request.NextPageToken); err != nil {
			return nil, serviceerror.NewInternal("page token is corrupted")
		}
	}

	params := table.NewQueryParameters(
		table.ValueParam("$expire_at_gt", types.TimestampValueFromTime(conn.ToYDBDateTime(time.Now()))),
	)
	var declares []string
	var suffixes []string
	if request.HostIDEquals != nil {
		declares = append(declares, m.client.HostIDDecl())
		params.Add(table.ValueParam("$host_id", m.client.HostIDValueFromUUID(request.HostIDEquals)))
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
		params.Add(table.ValueParam("$session_started_gt", types.TimestampValueFromTime(conn.ToYDBDateTime(request.SessionStartedAfter))))
		suffixes = append(suffixes, "AND session_start > $session_started_gt")
	}
	if request.LastHeartbeatWithin > 0 {
		declares = append(declares, "DECLARE $last_heartbeat_gt AS Timestamp;")
		params.Add(table.ValueParam("$last_heartbeat_gt", types.TimestampValueFromTime(conn.ToYDBDateTime(time.Now().Add(-request.LastHeartbeatWithin)))))
		suffixes = append(suffixes, "AND last_heartbeat > $last_heartbeat_gt")
	}
	if len(pageToken.LastSeenHostID) > 0 && request.HostIDEquals == nil {
		declares = append(declares, fmt.Sprintf("DECLARE $host_id_gt AS %s;", m.client.HostIDType()))
		params.Add(table.ValueParam("$host_id_gt", m.client.HostIDValueFromUUID(pageToken.LastSeenHostID)))
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
	res, err := m.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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
		member, err := m.scanClusterMember(res)
		if err != nil {
			return nil, err
		}
		members = append(members, member)
	}

	resp = &p.GetClusterMembersResponse{
		ActiveMembers: members,
	}
	if request.PageSize > 0 && len(members) == request.PageSize {
		nextPageToken := tokens.ClusterMembersPageToken{
			LastSeenHostID: primitives.UUID(members[len(members)-1].HostID),
		}
		resp.NextPageToken, err = nextPageToken.Serialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create next page token")
		}
	}
	return resp, nil
}

func (m *ClusterMetadataStore) UpsertClusterMembership(
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
			table.ValueParam("$expire_at_lt", types.TimestampValueFromTime(conn.ToYDBDateTime(time.Now()))),
		)
	})
	return conn.ConvertToTemporalError("PruneClusterMembership", err)
}

func (m *ClusterMetadataStore) GetName() string {
	return ydbPersistenceName
}

func (m *ClusterMetadataStore) Close() {
}

func (m *ClusterMetadataStore) scanClusterMember(res result.Result) (*p.ClusterMember, error) {
	var hostID, rpcAddress string
	var hostIDBytes []byte
	var rpcPort, role int32
	var sessionStart, lastHeartbeat, expireAt time.Time

	var hostIDScanner named.Value
	if m.client.UseBytesForHostIDs() {
		hostIDScanner = named.OptionalWithDefault("host_id", &hostIDBytes)
	} else {
		hostIDScanner = named.OptionalWithDefault("host_id", &hostID)
	}

	if err := res.ScanNamed(
		hostIDScanner,
		named.OptionalWithDefault("rpc_address", &rpcAddress),
		named.OptionalWithDefault("rpc_port", &rpcPort),
		named.OptionalWithDefault("role", &role),
		named.OptionalWithDefault("session_start", &sessionStart),
		named.OptionalWithDefault("last_heartbeat", &lastHeartbeat),
		named.OptionalWithDefault("expire_at", &expireAt),
	); err != nil {
		return nil, fmt.Errorf("failed to scan cluster member: %w", err)
	}

	if m.client.UseBytesForHostIDs() {
		hostID = uuid.UUID(hostIDBytes).String()
	}

	return &p.ClusterMember{
		HostID:        uuid.Parse(hostID),
		RPCAddress:    net.ParseIP(rpcAddress),
		RPCPort:       uint16(rpcPort),
		Role:          p.ServiceType(role),
		SessionStart:  conn.FromYDBDateTime(sessionStart),
		LastHeartbeat: conn.FromYDBDateTime(lastHeartbeat),
		RecordExpiry:  conn.FromYDBDateTime(expireAt),
	}, nil
}
