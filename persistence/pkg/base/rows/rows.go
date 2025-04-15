package rows

import (
	"time"

	v1 "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

type IDEventKey struct {
	ShardID     int32
	NamespaceID primitives.UUID
	WorkflowID  string
	RunID       primitives.UUID
	ItemType    int32
	ItemID      int64
}

type NameEventKey struct {
	ShardID     int32
	NamespaceID primitives.UUID
	WorkflowID  string
	RunID       primitives.UUID
	ItemType    int32
	ItemName    string
}

type ShardRow struct {
	ShardID       int32
	RangeID       int64
	Shard         []byte
	ShardEncoding v1.EncodingType
}

type CurrentExecutionRow struct {
	ShardID                int32
	NamespaceID            primitives.UUID
	WorkflowID             string
	CurrentRunID           primitives.UUID
	ExecutionState         []byte
	ExecutionStateEncoding v1.EncodingType
	LastWriteVersion       int64
	State                  int32
}

type WorkflowExecutionRow struct {
	ShardID                int32
	NamespaceID            primitives.UUID
	WorkflowID             string
	RunID                  primitives.UUID
	Execution              []byte
	ExecutionEncoding      v1.EncodingType
	ExecutionState         []byte
	ExecutionStateEncoding v1.EncodingType
	Checksum               []byte
	ChecksumEncoding       v1.EncodingType
	NextEventID            int64
	DBRecordVersion        int64
}

type IdentifiedItemRow struct {
	ShardID      int32
	NamespaceID  primitives.UUID
	WorkflowID   string
	RunID        primitives.UUID
	ItemType     int32
	ItemID       int64
	Data         []byte
	DataEncoding v1.EncodingType
}

type NamedItemRow struct {
	ShardID      int32
	NamespaceID  primitives.UUID
	WorkflowID   string
	RunID        primitives.UUID
	ItemType     int32
	ItemName     string
	Data         []byte
	DataEncoding v1.EncodingType
}

type SignalRequestedItemRow struct {
	ShardID     int32
	NamespaceID primitives.UUID
	WorkflowID  string
	RunID       primitives.UUID
	ItemName    string
}

type ImmediateTaskRow struct {
	ShardID      int32
	CategoryID   int32
	ID           int64
	Data         []byte
	DataEncoding v1.EncodingType
}

type ScheduledTaskRow struct {
	ShardID      int32
	CategoryID   int32
	ID           int64
	VisibilityTS time.Time
	Data         []byte
	DataEncoding v1.EncodingType
}

type NodeRow struct {
	ShardID      int32
	TreeID       string
	BranchID     string
	NodeID       int64
	PrevTxnID    int64
	TxnID        int64
	Data         []byte
	DataEncoding v1.EncodingType
}

type TreeRow struct {
	ShardID        int32
	TreeID         string
	BranchID       string
	Branch         []byte
	BranchEncoding v1.EncodingType
}
