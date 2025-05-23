package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	NexusEndpointStore struct {
		client *conn.Client
		logger log.Logger
	}
)

const tableVersionEndpointID = `00000000-0000-0000-0000-000000000000`

const (
	rowTypePartitionStatus = iota
	rowTypeNexusEndpoint
)

func NewNexusEndpointStore(
	client *conn.Client,
	logger log.Logger,
) (p.NexusEndpointStore, error) {
	return &NexusEndpointStore{
		client: client,
		logger: logger,
	}, nil
}

func (s *NexusEndpointStore) Close() {
}

func (s *NexusEndpointStore) GetName() string {
	return ydbPersistenceName
}

func (s *NexusEndpointStore) CreateOrUpdateNexusEndpoint(ctx context.Context, request *p.InternalCreateOrUpdateNexusEndpointRequest) error {
	declare := `
DECLARE $partition_status_type AS Int32;
DECLARE $table_version_id AS String;
DECLARE $table_expected_version AS Int64;
DECLARE $table_new_version AS Int64;

DECLARE $endpoint_type AS Int32;
DECLARE $endpoint_id AS String;
DECLARE $endpoint_expected_version AS Int64;
DECLARE $endpoint_new_version AS Int64;
DECLARE $endpoint_data AS String;
DECLARE $endpoint_data_encoding AS Int16;

DISCARD SELECT $table_expected_version, $endpoint_expected_version;
`

	var endpointTemplate string
	if request.Endpoint.Version == 0 {
		endpointTemplate = `
INSERT INTO nexus_endpoints(type, id, data, data_encoding, version)
VALUES($endpoint_type, $endpoint_id, $endpoint_data, $endpoint_data_encoding, $endpoint_new_version)
;
`
	} else {
		endpointTemplate = `
DISCARD SELECT Ensure(version, version == $endpoint_expected_version, "ENDPOINT_VERSION_MISMATCH")
FROM nexus_endpoints
WHERE type = $endpoint_type AND id = $endpoint_id
;

UPDATE nexus_endpoints
SET data = $endpoint_data, data_encoding = $endpoint_data_encoding, version = $endpoint_new_version
WHERE type = $endpoint_type AND id = $endpoint_id
;
`
	}

	var versionTemplate string
	if request.LastKnownTableVersion == 0 {
		versionTemplate = `
INSERT INTO nexus_endpoints(type, id, version)
VALUES ($partition_status_type, $table_version_id, $table_new_version)
;
`
	} else {
		versionTemplate = `
DISCARD SELECT Ensure(version, version == $table_expected_version, "TABLE_VERSION_MISMATCH")
FROM nexus_endpoints
WHERE type = $partition_status_type AND id = $table_version_id
;

UPDATE nexus_endpoints
SET version = $table_new_version
WHERE type = $partition_status_type AND id = $table_version_id
;
`
	}

	template := s.client.AddQueryPrefix(declare + endpointTemplate + versionTemplate)
	params := table.NewQueryParameters(
		// table
		table.ValueParam("$partition_status_type", types.Int32Value(rowTypePartitionStatus)),
		table.ValueParam("$table_version_id", types.StringValueFromString(tableVersionEndpointID)),
		table.ValueParam("$table_expected_version", types.Int64Value(request.LastKnownTableVersion)),
		table.ValueParam("$table_new_version", types.Int64Value(request.LastKnownTableVersion+1)),
		// endpoint
		table.ValueParam("$endpoint_type", types.Int32Value(rowTypeNexusEndpoint)),
		table.ValueParam("$endpoint_id", types.BytesValue([]byte(request.Endpoint.ID))),
		table.ValueParam("$endpoint_expected_version", types.Int64Value(request.Endpoint.Version)),
		table.ValueParam("$endpoint_new_version", types.Int64Value(request.Endpoint.Version+1)),
		table.ValueParam("$endpoint_data", types.BytesValue(request.Endpoint.Data.Data)),
		table.ValueParam("$endpoint_data_encoding", s.client.NewEncodingTypeValue(request.Endpoint.Data.EncodingType)),
	)

	err := s.client.Write(ctx, template, params, table.WithIdempotent())
	if err != nil {
		if conn.IsPreconditionFailedAndContains(err, "ENDPOINT_VERSION_MISMATCH") || conn.IsPreconditionFailedAndContains(err, "Conflict with existing key") {
			return p.ErrNexusEndpointVersionConflict
		} else if conn.IsPreconditionFailedAndContains(err, "TABLE_VERSION_MISMATCH") {
			return p.ErrNexusTableVersionConflict
		}
		return conn.ConvertToTemporalError("CreateOrUpdateNexusEndpoint", err)
	}

	return nil
}

func (s *NexusEndpointStore) DeleteNexusEndpoint(ctx context.Context, request *p.DeleteNexusEndpointRequest) error {
	template := s.client.AddQueryPrefix(`
DECLARE $partition_status_type AS Int32;
DECLARE $table_version_id AS String;
DECLARE $table_expected_version AS Int64;
DECLARE $table_new_version AS Int64;

DECLARE $endpoint_type AS Int32;
DECLARE $endpoint_id AS String;

DISCARD SELECT Ensure(version, version == $table_expected_version, "TABLE_VERSION_MISMATCH")
FROM nexus_endpoints
WHERE type = $partition_status_type AND id = $table_version_id
;

DISCARD SELECT Ensure(0, Count(*) > 0, "ENDPOINT_DOES_NOT_EXIST")
FROM nexus_endpoints
WHERE type = $endpoint_type AND id = $endpoint_id
;

UPDATE nexus_endpoints
SET version = $table_new_version
WHERE type = $partition_status_type AND id = $table_version_id
;

DELETE FROM nexus_endpoints
WHERE type = $endpoint_type AND id = $endpoint_id
;
`)

	params := table.NewQueryParameters(
		// table
		table.ValueParam("$partition_status_type", types.Int32Value(rowTypePartitionStatus)),
		table.ValueParam("$table_version_id", types.StringValueFromString(tableVersionEndpointID)),
		table.ValueParam("$table_expected_version", types.Int64Value(request.LastKnownTableVersion)),
		table.ValueParam("$table_new_version", types.Int64Value(request.LastKnownTableVersion+1)),
		// endpoint
		table.ValueParam("$endpoint_type", types.Int32Value(rowTypeNexusEndpoint)),
		table.ValueParam("$endpoint_id", types.BytesValue([]byte(request.ID))),
	)

	err := s.client.Write(ctx, template, params, table.WithIdempotent())
	if err != nil {
		if conn.IsPreconditionFailedAndContains(err, "ENDPOINT_DOES_NOT_EXIST") {
			return serviceerror.NewNotFound(fmt.Sprintf("nexus endpoint not found for ID: %v", request.ID))
		} else if conn.IsPreconditionFailedAndContains(err, "TABLE_VERSION_MISMATCH") {
			return p.ErrNexusTableVersionConflict
		}
		return conn.ConvertToTemporalError("DeleteNexusEndpoint", err)
	}

	return nil
}

func (s *NexusEndpointStore) GetNexusEndpoint(ctx context.Context, request *p.GetNexusEndpointRequest) (resp *p.InternalNexusEndpoint, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("GetNexusEndpoint", err)
		}
	}()

	template := s.client.AddQueryPrefix(`
DECLARE $endpoint_type AS Int32;
DECLARE $endpoint_id AS String;

SELECT data, data_encoding, version
FROM nexus_endpoints
WHERE type = $endpoint_type AND id = $endpoint_id
LIMIT 1
`)

	params := table.NewQueryParameters(
		table.ValueParam("$endpoint_type", types.Int32Value(rowTypeNexusEndpoint)),
		table.ValueParam("$endpoint_id", types.BytesValue([]byte(request.ID))),
	)

	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
	if err != nil {
		return nil, conn.ConvertToTemporalError("GetNexusEndpoint", err)
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
		return nil, conn.WrapErrorAsRootCause(
			serviceerror.NewNotFound(fmt.Sprintf("Nexus incoming service with ID `%v` not found", request.ID)),
		)
	}

	resp = &p.InternalNexusEndpoint{
		ID: request.ID,
	}
	var data []byte
	var encodingType conn.EncodingTypeRaw

	err = res.ScanNamed(
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", &encodingType),
		named.OptionalWithDefault("version", &resp.Version),
	)
	if err != nil {
		return nil, err
	}

	resp.Data = p.NewDataBlob(data, enumspb.EncodingType(encodingType).String())
	return resp, nil
}

func (s *NexusEndpointStore) ListNexusEndpoints(ctx context.Context, request *p.ListNexusEndpointsRequest) (resp *p.InternalListNexusEndpointsResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("ListNexusEndpoints", err)
		}
	}()

	var token tokens.NexusEndpointsPageToken
	token.Deserialize(request.NextPageToken)

	template := s.client.AddQueryPrefix(`
DECLARE $partition_status_type AS Int32;
DECLARE $table_version_id AS String;

DECLARE $endpoint_type AS Int32;
DECLARE $endpoint_last_seen_id AS String;
DECLARE $page_size AS Uint64;

SELECT type, id, data, data_encoding, version
FROM nexus_endpoints
WHERE type = $partition_status_type AND id = $table_version_id
;

SELECT type, id, data, data_encoding, version
FROM nexus_endpoints
WHERE type = $endpoint_type AND id > $endpoint_last_seen_id
ORDER BY id
LIMIT $page_size
;
`)

	params := table.NewQueryParameters(
		// table
		table.ValueParam("$partition_status_type", types.Int32Value(rowTypePartitionStatus)),
		table.ValueParam("$table_version_id", types.StringValueFromString(tableVersionEndpointID)),
		// endpoint
		table.ValueParam("$endpoint_type", types.Int32Value(rowTypeNexusEndpoint)),
		table.ValueParam("$endpoint_last_seen_id", types.StringValueFromString(token.LastSeenEndpointID)),
		table.ValueParam("$page_size", types.Uint64Value(uint64(request.PageSize))),
	)

	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
	if err != nil {
		return nil, conn.ConvertToTemporalError("ListNexusEndpoints", err)
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
		return &p.InternalListNexusEndpointsResponse{}, nil
	}

	var tableVersion int64
	err = res.ScanNamed(
		named.OptionalWithDefault("version", &tableVersion),
	)
	if err != nil {
		return nil, err
	}
	if request.LastKnownTableVersion != 0 && tableVersion != request.LastKnownTableVersion {
		return nil, fmt.Errorf("%w. provided table version: %v current table version: %v",
			p.ErrNexusTableVersionConflict,
			request.LastKnownTableVersion,
			tableVersion)
	}

	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	var endpoints []p.InternalNexusEndpoint
	for res.NextRow() {
		var endpoint p.InternalNexusEndpoint
		var data []byte
		var encodingType conn.EncodingTypeRaw
		err = res.ScanNamed(
			named.OptionalWithDefault("id", &endpoint.ID),
			named.OptionalWithDefault("version", &endpoint.Version),
			named.OptionalWithDefault("data", &data),
			named.OptionalWithDefault("data_encoding", &encodingType),
		)
		if err != nil {
			return nil, err
		}
		endpoint.Data = p.NewDataBlob(data, enumspb.EncodingType(encodingType).String())
		endpoints = append(endpoints, endpoint)
	}

	var nextPageToken []byte
	if len(endpoints) == request.PageSize {
		token := tokens.NexusEndpointsPageToken{
			LastSeenEndpointID: endpoints[len(endpoints)-1].ID,
		}
		nextPageToken = token.Serialize()
	}

	return &p.InternalListNexusEndpointsResponse{
		TableVersion:  tableVersion,
		NextPageToken: nextPageToken,
		Endpoints:     endpoints,
	}, nil
}

var _ p.NexusEndpointStore = (*NexusEndpointStore)(nil)
