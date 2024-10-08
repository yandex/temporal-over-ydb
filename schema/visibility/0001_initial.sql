-- +goose Up
CREATE TABLE executions_visibility
(
    namespace_id Utf8,
    run_id Utf8,
    start_time     Timestamp,
    execution_time Timestamp,
    workflow_id Utf8,
    workflow_type_name Utf8,
    status Int32,
    close_time     Timestamp,
    history_length Int64,
    memo String,
    memo_encoding Utf8,
    task_queue Utf8,

    PRIMARY KEY (namespace_id, run_id)
)
WITH ( AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- TODO indices

-- +goose Down
DROP TABLE executions_visibility;