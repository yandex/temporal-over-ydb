package persistencelite

import (
	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/rows"
)

func prepareDeleteBufferedEventsQuery(shardID int32, namespaceID, workflowID, runID, condID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
DELETE FROM events
WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? AND event_type = ? AND
(SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{shardID, namespaceID, workflowID, runID, rows.EventTypeBufferedEvent, condID},
	}
}

func prepareRemoveAllEventsForIDQuery(shardID int32, namespaceID, workflowID, runID, condID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
DELETE FROM events
WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ? AND
(SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{shardID, namespaceID, workflowID, runID, condID},
	}
}
