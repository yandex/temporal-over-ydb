-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS queue_v2
(
    queue_type       Int32,
    queue_name       Utf8,
    metadata_payload  String,
    metadata_encoding Int16,
    version           Int64,
    PRIMARY KEY (queue_type, queue_name)
) WITH (
       AUTO_PARTITIONING_BY_SIZE = ENABLED,
       AUTO_PARTITIONING_BY_LOAD = ENABLED
       );
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS queue_v2_message
(
    queue_type       Int32,
    queue_name       Utf8,
    queue_partition  Int32,

    message_id       Int64,
    message_payload  String,
    message_encoding Int16,
    PRIMARY KEY (queue_type, queue_name, queue_partition, message_id)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );
-- +goose StatementEnd
