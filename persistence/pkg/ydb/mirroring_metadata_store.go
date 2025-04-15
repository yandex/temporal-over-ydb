package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	p "go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	MirroringMetadataStore struct {
		client *conn.Client
	}
)

// NewMirroringMetadataStore is used to create an instance of the Namespace MirroringMetadataStore implementation
func NewMirroringMetadataStore(
	client *conn.Client,
) (*MirroringMetadataStore, error) {
	return &MirroringMetadataStore{
		client: client,
	}, nil
}

func (m *MirroringMetadataStore) UpsertNamespace(
	ctx context.Context,
	request *p.InternalUpdateNamespaceRequest,
) error {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)

		template := m.client.AddQueryPrefix(m.client.NamspaceIDDecl() + `
DECLARE $detail AS string;
DECLARE $detail_encoding AS ` + m.client.EncodingType().String() + `;
DECLARE $notification_version AS int64;
DECLARE $is_global_namespace AS bool;
DECLARE $name AS utf8;
DECLARE $metadata_record_name AS utf8;

UPSERT INTO namespaces_by_id (id, name)
VALUES ($namespace_id, $name);

UPSERT INTO namespaces (id, name, detail, detail_encoding, is_global_namespace, notification_version)
VALUES ($namespace_id, $name, $detail, $detail_encoding, $is_global_namespace, $notification_version);
`)
		params := table.NewQueryParameters(
			table.ValueParam("$namespace_id", m.client.NamespaceIDValue(request.Id)),
			table.ValueParam("$name", types.UTF8Value(request.Name)),
			table.ValueParam("$detail", types.BytesValue(request.Namespace.Data)),
			table.ValueParam("$detail_encoding", m.client.EncodingTypeValue(request.Namespace.EncodingType)),
			table.ValueParam("$notification_version", types.Int64Value(request.NotificationVersion)),
			table.ValueParam("$is_global_namespace", types.BoolValue(request.IsGlobal)),
			table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
		)
		return e.Write(ctx, template, params)
	})
	return conn.ConvertToTemporalError("UpsertNamespace", err)
}

func (m *MirroringMetadataStore) SetMetadata(ctx context.Context, notificationVersion int64) error {
	err := m.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		e := conn.NewExecutorFromTransactionActor(tx)

		template := m.client.AddQueryPrefix(`
DECLARE $notification_version AS int64;
DECLARE $metadata_record_name AS utf8;

UPSERT INTO namespaces (name, notification_version)
VALUES ($metadata_record_name, $notification_version);
`)
		params := table.NewQueryParameters(
			table.ValueParam("$notification_version", types.Int64Value(notificationVersion)),
			table.ValueParam("$metadata_record_name", types.UTF8Value(namespaceMetadataRecordName)),
		)
		return e.Write(ctx, template, params)
	})
	if err != nil {
		return conn.ConvertToTemporalError("SetMetadata", err)
	}
	return nil
}
