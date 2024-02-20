package rows

import (
	"time"

	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
)

type IdentifiedEventKey struct {
	ShardID     int32
	NamespaceID string
	WorkflowID  string
	RunID       string
	EventType   int32
	EventID     int64
}

func (k IdentifiedEventKey) ToDeleteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			DELETE FROM events
			WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? AND event_type = ? AND event_id = ? AND event_name = ? AND
			(SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			k.ShardID,
			k.NamespaceID,
			k.WorkflowID,
			k.RunID,
			k.EventType,
			k.EventID,
			"",
			conditionID,
		},
	}
}

type NamedEventKey struct {
	ShardID     int32
	NamespaceID string
	WorkflowID  string
	RunID       string
	EventType   int32
	EventName   string
}

func (k NamedEventKey) ToDeleteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			DELETE FROM events
			WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? AND event_type = ? AND event_id = ? AND event_name = ? AND
			(SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			k.ShardID,
			k.NamespaceID,
			k.WorkflowID,
			k.RunID,
			k.EventType,
			0,
			k.EventName,
			conditionID,
		},
	}
}

type Deletable interface {
	ToDeleteQuery(conditionID string) gorqlite.ParameterizedStatement
}

type Upsertable interface {
	ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement
}

type ShardRow struct {
	ShardID       int32
	RangeID       int64
	Shard         []byte
	ShardEncoding string
}

type CurrentExecutionRow struct {
	ShardID                int32
	NamespaceID            string
	WorkflowID             string
	CurrentRunID           string
	ExecutionState         []byte
	ExecutionStateEncoding string
	LastWriteVersion       int64
	State                  int32
}

func (r *CurrentExecutionRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT OR REPLACE INTO current_executions (shard_id, namespace_id, workflow_id, current_run_id, execution_state, execution_state_encoding, last_write_version, state)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.NamespaceID,
			r.WorkflowID,
			r.CurrentRunID,
			r.ExecutionState,
			r.ExecutionStateEncoding,
			r.LastWriteVersion,
			r.State,
			conditionID,
		},
	}
}

type WorkflowRow struct {
	ShardID                int32
	NamespaceID            string
	WorkflowID             string
	RunID                  string
	Execution              []byte
	ExecutionEncoding      string
	ExecutionState         []byte
	ExecutionStateEncoding string
	Checksum               []byte
	ChecksumEncoding       string
	NextEventID            int64
	DBRecordVersion        int64
}

func (r *WorkflowRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT OR REPLACE INTO executions (shard_id, namespace_id, workflow_id, run_id, execution, execution_encoding, execution_state, execution_state_encoding, checksum, checksum_encoding, next_event_id, db_record_version)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.NamespaceID,
			r.WorkflowID,
			r.RunID,
			r.Execution,
			r.ExecutionEncoding,
			r.ExecutionState,
			r.ExecutionStateEncoding,
			r.Checksum,
			r.ChecksumEncoding,
			r.NextEventID,
			r.DBRecordVersion,
			conditionID,
		},
	}
}

type IdentifiedEventRow struct {
	ShardID      int32
	NamespaceID  string
	WorkflowID   string
	RunID        string
	EventType    int32
	EventID      int64
	Data         []byte
	DataEncoding string
}

func (r *IdentifiedEventRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT OR REPLACE INTO events (shard_id, namespace_id, workflow_id, run_id, event_type, event_id, event_name, data, data_encoding)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.NamespaceID,
			r.WorkflowID,
			r.RunID,
			r.EventType,
			r.EventID,
			"",
			r.Data,
			r.DataEncoding,
			conditionID,
		},
	}
}

type NamedEventRow struct {
	ShardID      int32
	NamespaceID  string
	WorkflowID   string
	RunID        string
	EventType    int32
	EventName    string
	Data         []byte
	DataEncoding string
}

func (r *NamedEventRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT OR REPLACE INTO events (shard_id, namespace_id, workflow_id, run_id, event_type, event_id, event_name, data, data_encoding)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.NamespaceID,
			r.WorkflowID,
			r.RunID,
			r.EventType,
			0,
			r.EventName,
			r.Data,
			r.DataEncoding,
			conditionID,
		},
	}
}

type SignalRequestedRow struct {
	ShardID     int32
	NamespaceID string
	WorkflowID  string
	RunID       string
	EventName   string
}

func (r *SignalRequestedRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT OR REPLACE INTO events (shard_id, namespace_id, workflow_id, run_id, event_type, event_id, event_name)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.NamespaceID,
			r.WorkflowID,
			r.RunID,
			EventTypeSignalRequested,
			0,
			r.EventName,
			conditionID,
		},
	}
}

type ImmediateTaskRow struct {
	ShardID      int32
	CategoryID   int32
	ID           int64
	Data         []byte
	DataEncoding string
}

func (r *ImmediateTaskRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO tasks (shard_id, category_id, visibility_ts, id, data, data_encoding)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.CategoryID,
			0,
			r.ID,
			r.Data,
			r.DataEncoding,
			conditionID,
		},
	}
}

type ScheduledTaskRow struct {
	ShardID      int32
	CategoryID   int32
	ID           int64
	VisibilityTS time.Time
	Data         []byte
	DataEncoding string
}

func (r *ScheduledTaskRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO tasks (shard_id, category_id, visibility_ts, id, data, data_encoding)
			SELECT * FROM (VALUES
				(?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{
			r.ShardID,
			r.CategoryID,
			conn.ToSQLiteDateTime(r.VisibilityTS),
			r.ID,
			r.Data,
			r.DataEncoding,
			conditionID,
		},
	}
}
