package ydb

import (
	"context"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	QueueStoreV2 struct {
		queueType persistence.QueueType
		client    *conn.Client
		logger    log.Logger
	}

	queue struct {
		Metadata *persistencespb.Queue
		Version  int64
	}
)

func NewQueueStoreV2(
	client *conn.Client,
	logger log.Logger,
) (persistence.QueueV2, error) {
	return &QueueStoreV2{
		client: client,
		logger: logger,
	}, nil
}

func (s *QueueStoreV2) Close() {
}

func (s *QueueStoreV2) EnqueueMessage(ctx context.Context, request *persistence.InternalEnqueueMessageRequest) (resp *persistence.InternalEnqueueMessageResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("EnqueueMessage", err)
		}
	}()
	// TODO: add concurrency control around this method to avoid things like QueueMessageIDConflict.
	// TODO: cache the queue in memory to avoid querying the database every time.
	_, err = s.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	messageID, err := s.getNextMessageID(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	template := s.client.AddQueryPrefix(`
		DECLARE $queue_type AS Int32;
		DECLARE $queue_name AS Utf8;
		DECLARE $queue_partition AS Int32;
		DECLARE $message_id AS Int64;
		DECLARE $message_payload AS String;
		DECLARE $message_encoding AS ` + s.client.EncodingType().String() + `;

		INSERT INTO queue_v2_message (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding)
		VALUES ($queue_type, $queue_name, $queue_partition, $message_id, $message_payload, $message_encoding);
		`)

	err = s.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(request.QueueType))),
		table.ValueParam("$queue_name", types.UTF8Value(request.QueueName)),
		table.ValueParam("$queue_partition", types.Int32Value(0)),
		table.ValueParam("$message_id", types.Int64Value(messageID)),
		table.ValueParam("$message_payload", types.BytesValue(request.Blob.Data)),
		table.ValueParam("$message_encoding", s.client.EncodingTypeValue(request.Blob.EncodingType)),
	))
	if err != nil {
		if ydb.IsOperationErrorAlreadyExistsError(err) || conn.IsPreconditionFailedAndContains(err, "Conflict with existing key") {
			return &persistence.InternalEnqueueMessageResponse{
				Metadata: persistence.MessageMetadata{ID: messageID},
			}, nil
		}
		return nil, err
	}

	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: messageID},
	}, nil
}

func (s *QueueStoreV2) ReadMessages(ctx context.Context, request *persistence.InternalReadMessagesRequest) (resp *persistence.InternalReadMessagesResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("ReadMessages", err)
	}()

	q, err := s.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	if request.PageSize <= 0 {
		return nil, conn.WrapErrorAsRootCause(persistence.ErrNonPositiveReadQueueMessagesPageSize)
	}
	minMessageID, err := persistence.GetMinMessageIDToReadForQueueV2(request.QueueType, request.QueueName, request.NextPageToken, q.Metadata)
	if err != nil {
		return nil, conn.WrapErrorAsRootCause(err)
	}

	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $message_id_gt AS Int64;
DECLARE $page_size AS Int32;

SELECT message_id, message_payload, message_encoding
FROM queue_v2_message
WHERE queue_type = $queue_type
AND queue_name = $queue_name
AND queue_partition = 0
AND message_id >= $message_id_gt
ORDER BY message_id ASC
LIMIT $page_size;
`)
	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(request.QueueType))),
		table.ValueParam("$queue_name", types.UTF8Value(request.QueueName)),
		table.ValueParam("$queue_partition", types.Int32Value(0)),
		table.ValueParam("$message_id_gt", types.Int64Value(minMessageID)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
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

	messages := make([]persistence.QueueV2Message, 0, request.PageSize)
	for res.NextRow() {
		message, err := s.scanQueueMessageV2(res)
		if err != nil {
			return nil, err
		}
		messages = append(messages, *message)
	}

	nextPageToken := persistence.GetNextPageTokenForReadMessages(messages)
	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *QueueStoreV2) CreateQueue(ctx context.Context, request *persistence.InternalCreateQueueRequest) (*persistence.InternalCreateQueueResponse, error) {
	q := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	bytes, _ := q.Marshal()

	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $metadata_payload AS String;
DECLARE $metadata_encoding AS ` + s.client.EncodingType().String() + `;
DECLARE $version AS Int64;

INSERT INTO queue_v2 (queue_type, queue_name, metadata_payload, metadata_encoding, version)
VALUES ($queue_type, $queue_name, $metadata_payload, $metadata_encoding, $version);
`)
	err := s.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(request.QueueType))),
		table.ValueParam("$queue_name", types.UTF8Value(request.QueueName)),
		table.ValueParam("$metadata_payload", types.BytesValue(bytes)),
		table.ValueParam("$metadata_encoding", s.client.EncodingTypeValue(enumspb.ENCODING_TYPE_PROTO3)),
		table.ValueParam("$version", types.Int64Value(0)),
	))
	if err != nil {
		if ydb.IsOperationErrorAlreadyExistsError(err) || conn.IsPreconditionFailedAndContains(err, "Conflict with existing key") {
			return nil, fmt.Errorf(
				"%w: queue type %v and name %v",
				persistence.ErrQueueAlreadyExists,
				request.QueueType,
				request.QueueName,
			)
		}
		return nil, conn.ConvertToTemporalError("CreateQueue", err)
	}
	return &persistence.InternalCreateQueueResponse{}, nil
}

func (s *QueueStoreV2) RangeDeleteMessages(ctx context.Context, request *persistence.InternalRangeDeleteMessagesRequest) (resp *persistence.InternalRangeDeleteMessagesResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("RangeDeleteMessages", err)
	}()

	if request.InclusiveMaxMessageMetadata.ID < persistence.FirstQueueMessageID {
		return nil, conn.WrapErrorAsRootCause(fmt.Errorf(
			"%w: id is %d but must be >= %d",
			persistence.ErrInvalidQueueRangeDeleteMaxMessageID,
			request.InclusiveMaxMessageMetadata.ID,
			persistence.FirstQueueMessageID,
		))
	}

	queueType := request.QueueType
	queueName := request.QueueName
	q, err := s.getQueue(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	partition, err := persistence.GetPartitionForQueueV2(queueType, queueName, q.Metadata)
	if err != nil {
		return nil, err
	}
	maxMessageID, ok, err := s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Nothing in the queue to delete.
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	deleteRange, ok := persistence.GetDeleteRange(persistence.DeleteRequest{
		LastIDToDeleteInclusive: request.InclusiveMaxMessageMetadata.ID,
		ExistingMessageRange: persistence.InclusiveMessageRange{
			MinMessageID: partition.MinMessageId,
			MaxMessageID: maxMessageID,
		},
	})
	if !ok {
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}

	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $queue_partition AS Int32;
DECLARE $message_id_lt AS Int64;
DECLARE $message_id_gte AS Int64;

DELETE FROM queue_v2_message
WHERE queue_type = $queue_type
AND queue_name = $queue_name
AND queue_partition = $queue_partition
AND message_id >= $message_id_gte
AND message_id < $message_id_lt;
`)
	err = s.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$queue_partition", types.Int32Value(0)),
		table.ValueParam("$message_id_gte", types.Int64Value(deleteRange.MinMessageID)),
		table.ValueParam("$message_id_lt", types.Int64Value(deleteRange.MaxMessageID)),
	))
	if err != nil {
		return nil, err
	}
	partition.MinMessageId = deleteRange.NewMinMessageID
	err = s.updateQueue(ctx, q, queueType, queueName)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalRangeDeleteMessagesResponse{
		MessagesDeleted: deleteRange.MessagesToDelete,
	}, nil
}

func (s *QueueStoreV2) ListQueues(ctx context.Context, request *persistence.InternalListQueuesRequest) (resp *persistence.InternalListQueuesResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("ListQueues", err)
	}()

	if request.PageSize <= 0 {
		return nil, conn.WrapErrorAsRootCause(persistence.ErrNonPositiveListQueuesPageSize)
	}

	var token tokens.QueueV2PageToken
	if err := token.Deserialize(request.NextPageToken); err != nil {
		return nil, err
	}

	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $page_size AS Int32;

SELECT queue_name, metadata_payload, metadata_encoding, version
FROM queue_v2
WHERE queue_type = $queue_type 
AND queue_name > $queue_name
ORDER BY queue_name ASC 
LIMIT $page_size;
`)
	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(request.QueueType))),
		table.ValueParam("$queue_name", types.UTF8Value(token.LastReadQueueName)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
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

	var nextPageToken tokens.QueueV2PageToken
	var queues []persistence.QueueInfo
	for res.NextRow() {
		var (
			queueName     string
			metadataBytes []byte
			encoding      string
			encodingType  conn.EncodingTypeRaw
			version       int64
		)

		var encodingScanner named.Value
		if s.client.UseIntForEncoding() {
			encodingScanner = named.OptionalWithDefault("metadata_encoding", &encodingType)
		} else {
			encodingScanner = named.OptionalWithDefault("metadata_encoding", &encoding)
		}

		err = res.ScanNamed(
			named.OptionalWithDefault("queue_name", &queueName),
			named.OptionalWithDefault("metadata_payload", &metadataBytes),
			encodingScanner,
			named.OptionalWithDefault("version", &version),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
		}
		if s.client.UseIntForEncoding() {
			encoding = enumspb.EncodingType(encodingType).String()
		}
		q, err := newQueueMetadataV2(request.QueueType, queueName, metadataBytes, encoding, version)
		if err != nil {
			return nil, err
		}
		partition, err := persistence.GetPartitionForQueueV2(request.QueueType, queueName, q.Metadata)
		if err != nil {
			return nil, err
		}
		nextMessageID, err := s.getNextMessageID(ctx, request.QueueType, queueName)
		if err != nil {
			return nil, err
		}
		messageCount := nextMessageID - partition.MinMessageId
		nextPageToken.LastReadQueueName = queueName
		queues = append(queues, persistence.QueueInfo{
			QueueName:    queueName,
			MessageCount: messageCount,
		})
	}
	resp = &persistence.InternalListQueuesResponse{
		Queues: queues,
	}
	if len(queues) == request.PageSize {
		resp.NextPageToken, err = nextPageToken.Serialize()
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *QueueStoreV2) scanQueueMessageV2(res result.Result) (*persistence.QueueV2Message, error) {
	var messageID int64
	var data []byte
	var encodingStr string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if s.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("message_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("message_encoding", &encodingStr)
	}
	if err := res.ScanNamed(
		named.OptionalWithDefault("message_id", &messageID),
		named.OptionalWithDefault("message_payload", &data),
		encodingScanner,
	); err != nil {
		return nil, fmt.Errorf("failed to scan queue message: %w", err)
	}
	if s.client.UseIntForEncoding() {
		encodingStr = enumspb.EncodingType(encodingType).String()
	}
	encoding, err := enumspb.EncodingTypeFromString(encodingStr)
	if err != nil {
		return nil, conn.WrapErrorAsRootCause(serialization.NewUnknownEncodingTypeError(encodingStr))
	}
	return &persistence.QueueV2Message{
		MetaData: persistence.MessageMetadata{ID: messageID},
		Data: &commonpb.DataBlob{
			EncodingType: encoding,
			Data:         data,
		},
	}, nil
}

func (s *QueueStoreV2) updateQueue(
	ctx context.Context,
	q *queue,
	queueType persistence.QueueV2Type,
	queueName string,
) error {
	bytes, _ := q.Metadata.Marshal()
	version := q.Version
	nextVersion := version + 1
	q.Version = nextVersion

	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $metadata_payload AS String;
DECLARE $metadata_encoding AS ` + s.client.EncodingType().String() + `;
DECLARE $version AS Int64;
DECLARE $prev_version AS Int64;

DISCARD SELECT Ensure(version, version == $prev_version, "VERSION_MISMATCH")
FROM queue_v2
WHERE queue_type = $queue_type
AND queue_name = $queue_name;

UPDATE queue_v2
SET metadata_payload = $metadata_payload,
metadata_encoding = $metadata_encoding,
version = $version
WHERE queue_type = $queue_type
AND queue_name = $queue_name;
`)
	if err := s.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$metadata_payload", types.BytesValue(bytes)),
		table.ValueParam("$metadata_encoding", s.client.EncodingTypeValue(enumspb.ENCODING_TYPE_PROTO3)),
		table.ValueParam("$version", types.Int64Value(nextVersion)),
		table.ValueParam("$prev_version", types.Int64Value(version)),
	)); err != nil {
		if conn.IsPreconditionFailedAndContains(err, "VERSION_MISMATCH") {
			return conn.WrapErrorAsRootCause(&persistence.ConditionFailedError{Msg: "concurrent write"})
		} else {
			return err
		}
	}
	return nil
}

func (s *QueueStoreV2) getQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	name string,
) (*queue, error) {
	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;

SELECT metadata_payload, metadata_encoding, version
FROM queue_v2
WHERE queue_type = $queue_type
AND queue_name = $queue_name;
`)
	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$queue_name", types.UTF8Value(name)),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}
	if !res.NextRow() {
		return nil, conn.WrapErrorAsRootCause(persistence.NewQueueNotFoundError(queueType, name))
	}
	return s.scanQueueMetadataV2(queueType, name, res)
}

func (s *QueueStoreV2) getNextMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, error) {
	maxMessageID, ok, err := s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}
	if !ok {
		return persistence.FirstQueueMessageID, nil
	}

	// The next message ID is the max message ID + 1.
	return maxMessageID + 1, nil
}

func (s *QueueStoreV2) getMaxMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, bool, error) {
	template := s.client.AddQueryPrefix(`
DECLARE $queue_type AS Int32;
DECLARE $queue_name AS Utf8;
DECLARE $queue_partition AS Int32;

SELECT message_id
FROM queue_v2_message
WHERE queue_type = $queue_type 
AND queue_name = $queue_name 
AND queue_partition = $queue_partition 
ORDER BY message_id DESC LIMIT 1;
`)
	res, err := s.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$queue_type", types.Int32Value(int32(queueType))),
		table.ValueParam("$queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$queue_partition", types.Int32Value(0)),
	), table.WithIdempotent())
	if err != nil {
		return 0, false, err
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return 0, false, err
	}
	if !res.NextRow() {
		return 0, false, nil
	}
	var maxMessageID int64
	if err = res.ScanNamed(
		named.OptionalWithDefault("message_id", &maxMessageID),
	); err != nil {
		return 0, false, fmt.Errorf("failed to scan message_id: %w", err)
	}
	return maxMessageID, true, nil
}

func newQueueMetadataV2(queueType persistence.QueueV2Type, queueName string, data []byte, encoding string, version int64) (*queue, error) {
	if encoding != enumspb.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"%w: invalid queue encoding type: queue with type %v and name %v has invalid encoding",
			serialization.NewUnknownEncodingTypeError(encoding, enumspb.ENCODING_TYPE_PROTO3),
			queueType,
			queueName,
		)
	}

	q := &persistencespb.Queue{}
	if err := q.Unmarshal(data); err != nil {
		return nil, serialization.NewDeserializationError(
			enumspb.ENCODING_TYPE_PROTO3,
			fmt.Errorf("%w: unmarshal queue payload: failed for queue with type %v and name %v",
				err, queueType, queueName),
		)
	}
	return &queue{
		Metadata: q,
		Version:  version,
	}, nil
}

func (s *QueueStoreV2) scanQueueMetadataV2(queueType persistence.QueueV2Type, queueName string, res result.Result) (*queue, error) {
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if s.client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("metadata_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("metadata_encoding", &encoding)
	}
	var version int64
	if err := res.ScanNamed(
		named.OptionalWithDefault("metadata_payload", &data),
		encodingScanner,
		named.OptionalWithDefault("version", &version),
	); err != nil {
		return nil, fmt.Errorf("failed to scan queue metadata: %w", err)
	}
	if s.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}
	return newQueueMetadataV2(queueType, queueName, data, encoding, version)
}
