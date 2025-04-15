package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	QueueStore struct {
		queueType persistence.QueueType
		client    *conn.Client
		logger    log.Logger
	}
)

func NewQueueStore(
	queueType persistence.QueueType,
	client *conn.Client,
	logger log.Logger,
) (persistence.Queue, error) {
	return &QueueStore{
		queueType: queueType,
		client:    client,
		logger:    logger,
	}, nil
}

func (q *QueueStore) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	if err := q.initializeQueueMetadata(ctx, blob); err != nil {
		return err
	}
	return q.initializeDLQMetadata(ctx, blob)
}

func (q *QueueStore) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.enqueue(ctx, q.queueType, blob)
	return conn.ConvertToTemporalError("EnqueueMessage", err)
}

func (q *QueueStore) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	messageID, err := q.enqueue(ctx, q.getDLQTypeFromQueueType(), blob)
	if err != nil {
		return persistence.EmptyQueueMessageID, conn.ConvertToTemporalError("EnqueueMessageToDLQ", err)
	}
	return messageID, nil
}

func (q *QueueStore) enqueue(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_payload AS String;
DECLARE $message_encoding AS ` + q.client.EncodingType().String() + `;

$message_id = SELECT message_id FROM queue WHERE queue_type = $queue_type ORDER BY message_id DESC LIMIT 1;
$new_message_id = SELECT IF($message_id IS NOT NULL, $message_id + 1, 0);

UPSERT INTO queue (queue_type, message_id, message_payload, message_encoding)
VALUES ($queue_type, $new_message_id, $message_payload, $message_encoding);

SELECT $new_message_id AS new_message_id;
`)
	res, err := q.client.Do(ctx, template, table.SerializableReadWriteTxControl(table.CommitTx()), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$message_payload", types.BytesValue(blob.Data)),
		table.ValueParam("$message_encoding", q.client.EncodingTypeValue(blob.EncodingType)),
	))
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	err = conn.EnsureOneRowCursor(ctx, res)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	var messageID *int64
	if err := res.Scan(&messageID); err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return *messageID, nil
}

func (q *QueueStore) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) (rv []*persistence.QueueMessage, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("ReadMessages", err)
	}()
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_id_gt AS Int64;
DECLARE $page_size AS Int32;

SELECT message_id, message_payload, message_encoding
FROM queue
WHERE queue_type = $queue_type
AND message_id > $message_id_gt
ORDER BY message_id ASC
LIMIT $page_size;
`)
	res, err := q.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(q.queueType))),
		table.ValueParam("$message_id_gt", types.Int64Value(lastMessageID)),
		table.ValueParam("$page_size", types.Int32Value(int32(maxCount))),
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

	messages := make([]*persistence.QueueMessage, 0, maxCount)
	for res.NextRow() {
		message, err := q.scanQueueMessage(res)
		if err != nil {
			return nil, err
		}
		message.QueueType = q.queueType
		messages = append(messages, message)
	}

	return messages, nil
}

func (q *QueueStore) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) (m []*persistence.QueueMessage, nextPageToken []byte, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("ReadMessagesFromDLQ", err)
	}()
	if len(pageToken) != 0 {
		var token tokens.TaskPageToken
		if err = token.Deserialize(pageToken); err != nil {
			return nil, nil, err
		}
		firstMessageID = token.TaskID
	}
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_id_gt AS Int64;
DECLARE $message_id_lte AS Int64;
DECLARE $page_size AS Int32;

SELECT message_id, message_payload, message_encoding
FROM queue WHERE queue_type = $queue_type
AND message_id > $message_id_gt
AND message_id <= $message_id_lte
ORDER BY message_id ASC
LIMIT $page_size;
`)
	res, err := q.client.Do(ctx, q.client.AddQueryPrefix(template), conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(q.getDLQTypeFromQueueType()))),
		table.ValueParam("$message_id_gt", types.Int64Value(firstMessageID)),
		table.ValueParam("$message_id_lte", types.Int64Value(lastMessageID)),
		table.ValueParam("$page_size", types.Int32Value(int32(pageSize))),
	), table.WithIdempotent())
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, nil, err
	}

	messages := make([]*persistence.QueueMessage, 0, pageSize)
	for res.NextRow() {
		message, err := q.scanQueueMessage(res)
		message.QueueType = q.getDLQTypeFromQueueType()
		if err != nil {
			return nil, nil, err
		}
		messages = append(messages, message)
	}
	if len(messages) >= pageSize {
		token := tokens.TaskPageToken{
			TaskID: messages[len(messages)-1].ID,
		}
		if nextPageToken, err = token.Serialize(); err != nil {
			return nil, nil, err
		}
	}
	return messages, nextPageToken, nil
}

func (q *QueueStore) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_id_lt AS Int64;

DELETE FROM queue
WHERE queue_type = $queue_type
AND message_id < $message_id_lt;
`)
	err := q.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(q.getDLQTypeFromQueueType()))),
		table.ValueParam("$message_id_lt", types.Int64Value(messageID)),
	))
	return conn.ConvertToTemporalError("DeleteMessagesBefore", err)
}

func (q *QueueStore) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_id AS Int64;

DELETE FROM queue
WHERE queue_type = $queue_type
AND message_id = $message_id;
`)
	err := q.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(q.getDLQTypeFromQueueType()))),
		table.ValueParam("$message_id", types.Int64Value(messageID)),
	))
	return conn.ConvertToTemporalError("DeleteMessageFromDLQ", err)
}

func (q *QueueStore) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $message_id_gt AS Int64;
DECLARE $message_id_lte AS Int64;

DELETE FROM queue
WHERE queue_type = $queue_type
AND message_id > $message_id_gt
AND message_id <= $message_id_lte;
`)
	err := q.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(q.getDLQTypeFromQueueType()))),
		table.ValueParam("$message_id_gt", types.Int64Value(firstMessageID)),
		table.ValueParam("$message_id_lte", types.Int64Value(lastMessageID)),
	))
	return conn.ConvertToTemporalError("RangeDeleteMessagesFromDLQ", err)
}

func (q *QueueStore) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) (err error) {
	defer func() {
		err = conn.ConvertToTemporalError("UpdateAckLevel", err)
	}()
	return q.updateAckLevel(ctx, metadata, q.queueType)
}

func (q *QueueStore) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	queueMetadata, err := q.mustSelectQueueMetadata(ctx, q.queueType)
	if err != nil {
		return nil, conn.ConvertToTemporalError("GetAckLevels", err)
	}
	return queueMetadata, nil
}

func (q *QueueStore) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) (err error) {
	defer func() {
		err = conn.ConvertToTemporalError("UpdateDLQAckLevel", err)
	}()
	return q.updateAckLevel(ctx, metadata, q.getDLQTypeFromQueueType())
}

func (q *QueueStore) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	// Use negative queue type as the dlq type
	queueMetadata, err := q.mustSelectQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if err != nil {
		return nil, conn.ConvertToTemporalError("GetDLQAckLevels", err)
	}
	return queueMetadata, nil
}

func (q *QueueStore) insertInitialQueueMetadataRecord(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) error {
	err := q.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;

SELECT queue_type FROM queue_metadata WHERE queue_type = $queue_type;
`)
		res, err := tx.Execute(ctx, template, table.NewQueryParameters(
			table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		))
		if err != nil {
			return err
		}
		if err = res.NextResultSetErr(ctx); err != nil {
			return err
		}
		if res.NextRow() {
			// means that the record exists already
			return nil
		}

		template = q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $data AS String;
DECLARE $encoding AS ` + q.client.EncodingType().String() + `;
DECLARE $version AS Int64;

INSERT INTO queue_metadata (queue_type, data, data_encoding, version)
VALUES ($queue_type, $data, $encoding, $version);
`)
		_, err = tx.Execute(ctx, template, table.NewQueryParameters(
			table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
			table.ValueParam("$data", types.BytesValue(blob.Data)),
			table.ValueParam("$encoding", q.client.EncodingTypeValue(blob.EncodingType)),
			table.ValueParam("$version", types.Int64Value(0)),
		))
		return err
	})
	return err
}

func (q *QueueStore) selectQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueType,
) (resp *persistence.InternalQueueMetadata, err error) {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;

SELECT data, data_encoding, version FROM queue_metadata WHERE queue_type = $queue_type;`)
	res, err := q.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
	), table.WithIdempotent())
	if err != nil {
		return
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}
	if !res.NextRow() {
		return nil, nil
	}
	return q.scanQueueMetadata(res)
}

func (q *QueueStore) mustSelectQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueType,
) (resp *persistence.InternalQueueMetadata, err error) {
	resp, err = q.selectQueueMetadata(ctx, queueType)
	if resp == nil {
		return nil, conn.WrapErrorAsRootCause(
			serviceerror.NewNotFound(fmt.Sprintf("queue metadata for %v does not exist", queueType)))
	}
	return
}

func (q *QueueStore) updateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
	queueType persistence.QueueType,
) (err error) {
	template := q.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $data AS String;
DECLARE $encoding AS ` + q.client.EncodingType().String() + `;
DECLARE $version AS Int64;
DECLARE $prev_version AS Int64;

DISCARD SELECT Ensure(version, version == $prev_version, "VERSION_MISMATCH")
FROM queue_metadata WHERE queue_type = $queue_type;

UPDATE queue_metadata
SET data = $data,
data_encoding = $encoding,
version = $version
WHERE queue_type = $queue_type;
`)
	if err = q.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$data", types.BytesValue(metadata.Blob.Data)),
		table.ValueParam("$encoding", q.client.EncodingTypeValue(metadata.Blob.EncodingType)),
		table.ValueParam("$version", types.Int64Value(metadata.Version+1)),
		table.ValueParam("$prev_version", types.Int64Value(metadata.Version)),
	)); err != nil {
		if conn.IsPreconditionFailedAndContains(err, "VERSION_MISMATCH") {
			return conn.WrapErrorAsRootCause(&persistence.ConditionFailedError{Msg: "concurrent write"})
		} else {
			return err
		}
	}
	return nil
}

func (q *QueueStore) Close() {
}

func (q *QueueStore) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *QueueStore) initializeQueueMetadata(ctx context.Context, blob *commonpb.DataBlob) error {
	resp, err := q.selectQueueMetadata(ctx, q.queueType)
	if err != nil {
		return err
	}
	if resp == nil {
		return q.insertInitialQueueMetadataRecord(ctx, q.queueType, blob)
	}
	return nil
}

func (q *QueueStore) initializeDLQMetadata(ctx context.Context, blob *commonpb.DataBlob) error {
	resp, err := q.selectQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if err != nil {
		return err
	}
	if resp == nil {
		return q.insertInitialQueueMetadataRecord(ctx, q.getDLQTypeFromQueueType(), blob)
	}
	return nil
}

func (q *QueueStore) scanQueueMessage(res result.Result) (*persistence.QueueMessage, error) {
	var messageID int64
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if q.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("message_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("message_encoding", &encoding)
	}
	if err := res.ScanNamed(
		named.OptionalWithDefault("message_id", &messageID),
		named.OptionalWithDefault("message_payload", &data),
		encodingScanner,
	); err != nil {
		return nil, fmt.Errorf("failed to scan queue message: %w", err)
	}
	if q.client.UseIntForEncoding() {
		encoding = enums.EncodingType(encodingType).String()
	}
	return &persistence.QueueMessage{
		ID:       messageID,
		Data:     data,
		Encoding: encoding,
	}, nil
}

func (q *QueueStore) scanQueueMetadata(res result.Result) (*persistence.InternalQueueMetadata, error) {
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if q.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("data_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("data_encoding", &encoding)
	}
	var version int64
	if err := res.ScanNamed(
		named.OptionalWithDefault("data", &data),
		encodingScanner,
		named.OptionalWithDefault("version", &version),
	); err != nil {
		return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
	}
	if q.client.UseIntForEncoding() {
		encoding = enums.EncodingType(encodingType).String()
	}
	return &persistence.InternalQueueMetadata{
		Version: version,
		Blob:    persistence.NewDataBlob(data, encoding),
	}, nil
}
