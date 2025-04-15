-- +goose Up
CREATE TABLE IF NOT EXISTS tasks_and_task_queues
(
    namespace_id        Utf8  NOT NULL,
    task_queue_name     Utf8  NOT NULL,
    task_queue_type     Int32 NOT NULL,
    task_id             Int64,
    expire_at           Timestamp,

    range_id            Int64,
    task                String,
    task_encoding       Utf8,
    task_queue          String,
    task_queue_encoding Utf8,
    PRIMARY KEY (namespace_id, task_queue_name, task_queue_type, task_id)
) WITH (
      TTL = Interval ("PT0S") ON expire_at,
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

--
-- This table stores the following rows:
-- PK (shard_id, "", "", "", NULL,  ...) - shard
-- PK (shard_id, namespace_id, workflow_id, "", ...) - current
-- PK (shard_id, namespace_id, workflow_id, run_id, NULL, ...) - workflow
-- PK (shard_id, namespace_id, workflow_id, run_id, NULL, NULL, NULL, event_type, event_id, event_id, ...) - event
-- PK (shard_id, NULL, NULL, NULL, task_category_id, task_visibility_ts, task_id, NULL, ...) - task

CREATE TABLE IF NOT EXISTS executions
(
    shard_id                 Uint32 NOT NULL,
    namespace_id             Utf8,
    workflow_id              Utf8,
    run_id                   Utf8,

    task_category_id         Int32,
    task_visibility_ts       Timestamp,
    task_id                  Int64,

    event_type               Int32,
    event_id                 Int64,
    event_name               Utf8,

    execution                String, -- workflow
    execution_encoding       Utf8,   -- workflow
    execution_state          String, -- workflow and current
    execution_state_encoding Utf8,   -- workflow and current
    checksum                 String, -- workflow
    checksum_encoding        Utf8,   -- workflow
    db_record_version        Int64,  -- workflow
    next_event_id            Int64,  -- workflow

    current_run_id           Utf8,   -- current
    last_write_version       Int64,  -- current
    state                    Int32,  -- current

    range_id                 Int64,  -- shard
    shard                    String, -- shard
    shard_encoding           Utf8,   -- shard

    data                     String, -- event and task
    data_encoding            Utf8,   -- event and task

    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type,
                 event_id, event_name)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED,
      AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8,
      UNIFORM_PARTITIONS = 8,
      AUTO_PARTITIONING_PARTITION_SIZE_MB = 4096,
      KEY_BLOOM_FILTER = ENABLED
      );

CREATE TABLE IF NOT EXISTS history_node
(
    tree_id       Utf8  NOT NULL, -- run_id if no reset, otherwise run_id of first run
    branch_id     Utf8  NOT NULL, -- changes in case of reset workflow. Conflict resolution can also change branch id.
    node_id       Int64 NOT NULL, -- == first eventID in a batch of events
    txn_id        Int64 NOT NULL, -- in case of multiple transactions on same node, we utilize highest transaction ID. Unique.
    prev_txn_id   Int64,          -- point to the previous node: event chaining
    data          String,         -- batch of workflow execution history events as a blob
    data_encoding Utf8,           -- protocol used for history serialization
    PRIMARY KEY (tree_id, branch_id, node_id, txn_id)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS history_tree
(
    tree_id         Utf8 NOT NULL,
    branch_id       Utf8 NOT NULL,
    branch          String,
    branch_encoding Utf8,
    PRIMARY KEY (tree_id, branch_id)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS replication_tasks
(
    shard_id            Uint32,
    source_cluster_name Utf8,
    task_id             Int64,
    data                String,
    data_encoding       Utf8,
    PRIMARY KEY (shard_id, source_cluster_name, task_id)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS cluster_metadata_info
(
    cluster_name       Utf8,
    data               String,
    data_encoding      Utf8,
    version            Int64,
    PRIMARY KEY (cluster_name)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS queue
(
    queue_type       Int32,
    message_id       Int64,
    message_payload  String,
    message_encoding Utf8,
    PRIMARY KEY (queue_type, message_id)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS queue_metadata
(
    queue_type    Int32,
    data          String,
    data_encoding Utf8,
    version       Int64,
    PRIMARY KEY (queue_type)
) WITH (
      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );


-- this table is only used for storage of mapping of namespace uuid to namespace name
CREATE TABLE IF NOT EXISTS namespaces_by_id
(
    id   Utf8,
    name Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS namespaces
(
    name                 Utf8,
    id                   Utf8,
    detail               String,
    detail_encoding      Utf8,
    is_global_namespace  Bool,
    notification_version Int64,
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS cluster_membership
(
    host_id              Utf8,
    rpc_address          Utf8,
    rpc_port             Int32,
    role                 Int32,
    session_start        Timestamp,
    last_heartbeat       Timestamp,
    expire_at            Timestamp,
    PRIMARY KEY (role, host_id)
) WITH (
      TTL = Interval ("PT0S") ON expire_at,

      AUTO_PARTITIONING_BY_SIZE = ENABLED,
      AUTO_PARTITIONING_BY_LOAD = ENABLED
      );

CREATE TABLE IF NOT EXISTS task_queue_user_data
(
    namespace_id    Utf8,
    task_queue_name Utf8,
    data            String,
    data_encoding   Utf8,
    version         Int64,
    PRIMARY KEY (namespace_id, task_queue_name)
);

CREATE TABLE IF NOT EXISTS build_id_to_task_queue
(
    namespace_id    Utf8,
    build_id        Utf8,
    task_queue_name Utf8,
    PRIMARY KEY (namespace_id, build_id, task_queue_name)
);
