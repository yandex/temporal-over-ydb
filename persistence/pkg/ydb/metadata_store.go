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
	"go.temporal.io/server/common/primitives"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

const (
	namespaceMetadataRecordName = "temporal-namespace-metadata"
)

type (
	MetadataStore struct {
		client             *conn.Client
		logger             log.Logger
		currentClusterName string
	}
)

// NewMetadataStore is used to create an instance of the Namespace MetadataStore implementation
func NewMetadataStore(
	currentClusterName string,
	client *conn.Client,
	logger log.Logger,
) (p.MetadataStore, error) {
	return &MetadataStore{
		currentClusterName: currentClusterName,
		client:             client,
		logger:             logger,
	}, nil
}

// CreateNamespace create a namespace
func (m *MetadataStore) CreateNamespace(ctx context.Context, request *p.InternalCreateNamespaceRequest) (*p.CreateNamespaceResponse, error) {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)
		notificationVersion, err := m.selectNotificationVersion(ctx, e)
		if err != nil {
			return err
		}

		name, err := m.selectNamespaceNameByID(ctx, e, request.ID)
		if err != nil {
			return err
		}
		if name != "" {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceAlreadyExists,
				fmt.Sprintf("namespace name %s is already used by %s", name, request.ID))
		}

		row, err := m.selectNamespaceRow(ctx, e, request.Name)
		if err != nil {
			return err
		}
		if row != nil {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceAlreadyExists,
				fmt.Sprintf("namespace named %s already exists", name))
		}

		template := m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
DECLARE $name AS utf8;

UPSERT INTO namespaces_by_id (id, name)
VALUES ($namespace_id, $name);
`)
		params := table.NewQueryParameters(
			table.ValueParam("$namespace_id", m.client.NamespaceIDValue(request.ID)),
			table.ValueParam("$name", types.UTF8Value(request.Name)),
		)
		err = e.Write(ctx, template, params)
		if err != nil {
			return err
		}

		template = m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
DECLARE $name AS utf8;
DECLARE $detail AS string;
DECLARE $detail_encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $notification_version AS int64;
DECLARE $is_global_namespace AS bool;
DECLARE $metadata_record_name AS utf8;

UPSERT INTO namespaces (id, name, detail, detail_encoding, notification_version, is_global_namespace)
VALUES ($namespace_id, $name, $detail, $detail_encoding, $notification_version, $is_global_namespace);

UPSERT INTO namespaces (name, notification_version)
VALUES ($metadata_record_name, $notification_version + 1);
`)
		params = table.NewQueryParameters(
			table.ValueParam("$namespace_id", m.client.NamespaceIDValue(request.ID)),
			table.ValueParam("$name", types.UTF8Value(request.Name)),
			table.ValueParam("$detail", types.BytesValue(request.Namespace.Data)),
			table.ValueParam("$detail_encoding", m.client.EncodingTypeValue(request.Namespace.EncodingType)),
			table.ValueParam("$notification_version", types.Int64Value(notificationVersion)),
			table.ValueParam("$is_global_namespace", types.BoolValue(request.IsGlobal)),
			table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
		)
		return e.Write(ctx, template, params)
	})
	if err != nil {
		return nil, conn.ConvertToTemporalError("CreateNamespace", err)
	}
	return &p.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *MetadataStore) UpdateNamespace(
	ctx context.Context,
	request *p.InternalUpdateNamespaceRequest,
) error {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)
		notificationVersion, err := m.selectNotificationVersion(ctx, e)
		if err != nil {
			return err
		}

		if notificationVersion != request.NotificationVersion {
			return conn.NewRootCauseError(
				serviceerror.NewFailedPrecondition,
				fmt.Sprintf("notification version: %d, expected notification version: %d",
					notificationVersion, request.NotificationVersion))
		}

		row, err := m.selectNamespaceRow(ctx, e, request.Name)
		if err != nil {
			return err
		}
		if row == nil {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceNotFound,
				fmt.Sprintf("namespace %s does not exists", request.Name))
		}

		template := m.client.AddQueryPrefix(`
DECLARE $detail AS string;
DECLARE $detail_encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $notification_version AS int64;
DECLARE $is_global_namespace AS bool;
DECLARE $name AS utf8;
DECLARE $metadata_record_name AS utf8;

UPSERT INTO namespaces (name, detail, detail_encoding, is_global_namespace, notification_version)
VALUES ($name, $detail, $detail_encoding, $is_global_namespace, $notification_version);

UPSERT INTO namespaces (name, notification_version)
VALUES ($metadata_record_name, $notification_version + 1);
`)
		params := table.NewQueryParameters(
			table.ValueParam("$name", types.UTF8Value(request.Name)),
			table.ValueParam("$detail", types.BytesValue(request.Namespace.Data)),
			table.ValueParam("$detail_encoding", m.client.EncodingTypeValue(request.Namespace.EncodingType)),
			table.ValueParam("$notification_version", types.Int64Value(request.NotificationVersion)),
			table.ValueParam("$is_global_namespace", types.BoolValue(request.IsGlobal)),
			table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
		)
		return e.Write(ctx, template, params)
	})
	return conn.ConvertToTemporalError("UpdateNamespace", err)
}

func (m *MetadataStore) RenameNamespace(ctx context.Context, request *p.InternalRenameNamespaceRequest) error {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)
		notificationVersion, err := m.selectNotificationVersion(ctx, e)
		if err != nil {
			return err
		}

		if notificationVersion != request.NotificationVersion {
			return conn.NewRootCauseError(
				serviceerror.NewFailedPrecondition,
				fmt.Sprintf("notification version: %d, expected notification version: %d",
					notificationVersion, request.NotificationVersion))
		}

		row, err := m.selectNamespaceRow(ctx, e, request.Name)
		if err != nil {
			return err
		}
		if row != nil {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceAlreadyExists,
				fmt.Sprintf("namespace %s already exists", request.Name))
		}

		name, err := m.selectNamespaceNameByID(ctx, e, request.Id)
		if err != nil {
			return err
		}
		if name == "" {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceNotFound,
				fmt.Sprintf("namespace %s does not exist", request.Id))
		}

		template := m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
DECLARE $name AS utf8;
DECLARE $previous_name AS utf8;
DECLARE $notification_version AS int64;
DECLARE $data AS string;
DECLARE $encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $is_global_namespace AS bool;
DECLARE $metadata_record_name AS utf8;

DELETE FROM namespaces WHERE name = $previous_name;

UPSERT INTO namespaces (id, name, detail, detail_encoding, notification_version, is_global_namespace)
VALUES ($namespace_id, $name, $data, $encoding, $notification_version, $is_global_namespace);

UPSERT INTO namespaces (name, notification_version)
VALUES ($metadata_record_name, $notification_version + 1);

UPSERT INTO namespaces_by_id (id, name)
VALUES ($namespace_id, $name);
`)
		params := table.NewQueryParameters(
			table.ValueParam("$name", types.UTF8Value(request.Name)),
			table.ValueParam("$previous_name", types.UTF8Value(request.PreviousName)),
			table.ValueParam("$namespace_id", m.client.NamespaceIDValue(request.Id)),
			table.ValueParam("$data", types.BytesValue(request.Namespace.Data)),
			table.ValueParam("$encoding", m.client.EncodingTypeValue(request.Namespace.EncodingType)),
			table.ValueParam("$is_global_namespace", types.BoolValue(request.IsGlobal)),
			table.ValueParam("$notification_version", types.Int64Value(request.NotificationVersion)),
			table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
		)
		return e.Write(ctx, template, params)
	})

	return conn.ConvertToTemporalError("RenameNamespace", err)
}

func (m *MetadataStore) GetNamespace(ctx context.Context, request *p.GetNamespaceRequest) (resp *p.InternalGetNamespaceResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("GetNamespace", err)
		}
	}()

	if len(request.ID) > 0 && len(request.Name) > 0 {
		return nil, conn.NewRootCauseError(serviceerror.NewInvalidArgument, "both ID and Name specified in request.Namespace")
	} else if len(request.ID) == 0 && len(request.Name) == 0 {
		return nil, conn.NewRootCauseError(serviceerror.NewInvalidArgument, "both ID and Name are empty in request.Namespace")
	}

	var row *namespaceRow
	err = m.client.DB.Table().Do(ctx, func(c context.Context, s table.Session) error {
		e := conn.NewExecutorFromSession(s, conn.OnlineReadOnlyTxControl())

		name := request.Name
		if len(request.ID) > 0 {
			name, err = m.selectNamespaceNameByID(ctx, e, request.ID)
			if err != nil {
				return err
			}
			if name == "" {
				return conn.NewRootCauseError(
					serviceerror.NewNamespaceNotFound,
					fmt.Sprintf("namespace %s does not exist", request.ID))
			}
		}

		row, err = m.selectNamespaceRow(c, e, name)
		if err != nil {
			return err
		}
		if row == nil {
			return conn.NewRootCauseError(
				serviceerror.NewNamespaceNotFound,
				fmt.Sprintf("namespace %s does not exist", name))
		}
		return nil
	}, table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	return &p.InternalGetNamespaceResponse{
		Namespace:           row.blob,
		IsGlobal:            row.isGlobalNamespace,
		NotificationVersion: row.notificationVersion,
	}, nil
}

func (m *MetadataStore) ListNamespaces(ctx context.Context, request *p.InternalListNamespacesRequest) (resp *p.InternalListNamespacesResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("ListNamespaces", err)
		}
	}()

	var pageToken primitives.UUID = request.NextPageToken
	template := m.client.AddQueryPrefix(`
DECLARE $id_gt AS ` + m.client.NamespaceIDType().String() + `;
DECLARE $page_size AS int32;

SELECT id, name, detail, detail_encoding, notification_version, is_global_namespace
FROM namespaces
WHERE id > $id_gt
ORDER BY id ASC
LIMIT $page_size;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$id_gt", m.client.NamespaceIDValueFromUUID(pageToken)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	)
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
	if err := res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	response := &p.InternalListNamespacesResponse{}

	var lastID string
	for res.NextRow() {
		row, err := m.scanNamespaceRow(res)
		if err != nil {
			return nil, err
		}
		lastID = row.id
		if row.name == namespaceMetadataRecordName {
			continue
		}
		response.Namespaces = append(response.Namespaces, &p.InternalGetNamespaceResponse{
			Namespace:           row.blob,
			IsGlobal:            row.isGlobalNamespace,
			NotificationVersion: row.notificationVersion,
		})
	}

	if len(response.Namespaces) >= request.PageSize {
		response.NextPageToken, err = primitives.ParseUUID(lastID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse last id: %w", err)
		}
	}
	return response, nil
}

func (m *MetadataStore) DeleteNamespace(ctx context.Context, request *p.DeleteNamespaceRequest) (err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("DeleteNamespace", err)
		}
	}()

	if _, err := primitives.ParseUUID(request.ID); err != nil {
		return err
	}
	return m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)
		name, err := m.selectNamespaceNameByID(ctx, e, request.ID)
		if err != nil {
			return err
		}
		if name == "" {
			return nil
		}
		return m.deleteNamespace(ctx, e, name, request.ID)
	})
}
func (m *MetadataStore) DeleteNamespaceByName(ctx context.Context, request *p.DeleteNamespaceByNameRequest) error {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)
		id, err := m.selectNamespaceIDByName(ctx, e, request.Name)
		if err != nil {
			return err
		}
		if id == "" {
			return nil
		}
		return m.deleteNamespace(ctx, e, request.Name, id)
	})
	return conn.ConvertToTemporalError("DeleteNamespaceByName", err)
}

func (m *MetadataStore) GetMetadata(ctx context.Context) (resp *p.GetMetadataResponse, err error) {
	var notificationVersion int64 = 0
	err = m.client.DB.Table().Do(ctx, func(c context.Context, s table.Session) error {
		e := conn.NewExecutorFromSession(s, conn.OnlineReadOnlyTxControl())
		notificationVersion, err = m.selectNotificationVersion(c, e)
		return err
	}, table.WithIdempotent())
	if err != nil {
		return nil, conn.ConvertToTemporalError("GetMetadata", err)
	}
	return &p.GetMetadataResponse{NotificationVersion: notificationVersion}, nil
}

type namespaceRow struct {
	id                  string
	name                string
	notificationVersion int64
	isGlobalNamespace   bool
	blob                *commonpb.DataBlob
}

func (m *MetadataStore) scanNamespaceRow(res result.Result) (*namespaceRow, error) {
	var name string
	var detail []byte
	var notificationVersion int64
	var isGlobalNamespace bool

	var id string
	var idBytes primitives.UUID

	var idNamedValue named.Value
	if m.client.UseBytesForNamespaceIDs() {
		idNamedValue = named.OptionalWithDefault("id", &idBytes)
	} else {
		idNamedValue = named.OptionalWithDefault("id", &id)
	}

	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if m.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("detail_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("detail_encoding", &encoding)
	}

	if err := res.ScanNamed(
		idNamedValue,
		named.OptionalWithDefault("name", &name),
		named.OptionalWithDefault("detail", &detail),
		encodingScanner,
		named.OptionalWithDefault("notification_version", &notificationVersion),
		named.OptionalWithDefault("is_global_namespace", &isGlobalNamespace),
	); err != nil {
		return nil, fmt.Errorf("failed to scan namespace row: %w", err)
	}

	if m.client.UseBytesForNamespaceIDs() {
		id = idBytes.String()
	}
	if m.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}

	return &namespaceRow{
		blob:                p.NewDataBlob(detail, encoding),
		id:                  id,
		name:                name,
		notificationVersion: notificationVersion,
		isGlobalNamespace:   isGlobalNamespace,
	}, nil
}

func (m *MetadataStore) selectNamespaceRow(ctx context.Context, e conn.Executor, name string) (row *namespaceRow, err error) {
	template := m.client.AddQueryPrefix(`
DECLARE $name AS utf8;

SELECT id, name, detail, detail_encoding, notification_version, is_global_namespace
FROM namespaces
WHERE name = $name;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$name", types.UTF8Value(name)),
	)
	res, err := e.Execute(ctx, template, params)
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
	return m.scanNamespaceRow(res)
}

func (m *MetadataStore) selectNamespaceNameByID(ctx context.Context, e conn.Executor, id string) (name string, err error) {
	template := m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
SELECT name
FROM namespaces_by_id
WHERE id = $namespace_id;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", m.client.NamespaceIDValue(id)),
	)
	res, err := e.Execute(ctx, template, params)
	if err != nil {
		return "", err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return "", err
	}
	if !res.NextRow() {
		return "", nil
	}
	if err = res.ScanNamed(
		named.OptionalWithDefault("name", &name),
	); err != nil {
		return "", err
	}
	return name, nil
}

func (m *MetadataStore) selectNamespaceIDByName(ctx context.Context, e conn.Executor, name string) (id string, err error) {
	template := m.client.AddQueryPrefix(`
DECLARE $name AS utf8;

SELECT id
FROM namespaces
WHERE name = $name;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$name", types.UTF8Value(name)),
	)
	res, err := e.Execute(ctx, template, params)
	if err != nil {
		return "", err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return "", err
	}
	if !res.NextRow() {
		return "", nil
	}

	var idBytes primitives.UUID
	var idNamedValue named.Value
	if m.client.UseBytesForNamespaceIDs() {
		idNamedValue = named.OptionalWithDefault("id", &idBytes)
	} else {
		idNamedValue = named.OptionalWithDefault("id", &id)
	}

	if err = res.ScanNamed(idNamedValue); err != nil {
		return "", fmt.Errorf("failed to scan id: %w", err)
	}
	if m.client.UseBytesForNamespaceIDs() {
		id = idBytes.String()
	}
	return id, nil
}

func (m *MetadataStore) deleteNamespace(ctx context.Context, e conn.Executor, name, id string) error {
	template := m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
DECLARE $name AS utf8;

DELETE FROM namespaces_by_id WHERE id = $namespace_id;
DELETE FROM namespaces WHERE name = $name;
`)
	return e.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$name", types.UTF8Value(name)),
		table.ValueParam("$namespace_id", m.client.NamespaceIDValue(id)),
	))
}

func scanNotificationVersion(res result.Result) (int64, error) {
	var name string
	var notificationVersion int64

	if err := res.ScanNamed(
		named.OptionalWithDefault("name", &name),
		named.OptionalWithDefault("notification_version", &notificationVersion),
	); err != nil {
		return 0, fmt.Errorf("failed to scan notification version row: %w", err)
	}
	if name != namespaceMetadataRecordName {
		return 0, fmt.Errorf("failed to scan notification version row: name != %s", namespaceMetadataRecordName)
	}
	return notificationVersion, nil
}

func (m *MetadataStore) selectNotificationVersion(ctx context.Context, e conn.Executor) (version int64, err error) {
	template := m.client.AddQueryPrefix(`
DECLARE $metadata_record_name as utf8;

SELECT name, notification_version
FROM namespaces
WHERE name = $metadata_record_name;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
	)
	res, err := e.Execute(ctx, template, params)
	if err != nil {
		return 0, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return 0, err
	}
	if !res.NextRow() {
		// can happen in the very beginning, i.e. when namespaces is initialized
		return 0, nil
	}
	return scanNotificationVersion(res)
}

func (m *MetadataStore) GetName() string {
	return ydbPersistenceName
}

func (m *MetadataStore) Close() {
}
