-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS nexus_endpoints (
    type            Int32, -- enum RowType { PartitionStatus, NexusEndpoint }
    id              String,
    data            String,
    data_encoding   Int16,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all endpoints.
    -- When type=NexusEndpoint contains the endpoint version used for optimistic concurrency.
    version         Int64,
    PRIMARY KEY (type, id)
) WITH (
   AUTO_PARTITIONING_BY_SIZE = ENABLED,
   AUTO_PARTITIONING_BY_LOAD = ENABLED
);
-- +goose StatementEnd
