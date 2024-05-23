package persistence

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const upsertRowsQueryID = "ins_rs"

var upsertRowsDecl = func(id string) string {
	return fmt.Sprintf("DECLARE $%s AS %s;", "rows_to_add_"+id, types.List(
		types.Struct(
			types.StructField("shard_id", types.TypeUint32),
			types.StructField("namespace_id", types.Optional(types.TypeUTF8)),
			types.StructField("workflow_id", types.Optional(types.TypeUTF8)),
			types.StructField("run_id", types.Optional(types.TypeUTF8)),
			types.StructField("task_category_id", types.Optional(types.TypeInt32)),
			types.StructField("task_id", types.Optional(types.TypeInt64)),
			types.StructField("event_type", types.Optional(types.TypeInt32)),
			types.StructField("event_id", types.Optional(types.TypeInt64)),
			types.StructField("event_name", types.Optional(types.TypeUTF8)),
			types.StructField("task_visibility_ts", types.Optional(types.TypeTimestamp)),
			types.StructField("data", types.Optional(types.TypeBytes)),
			types.StructField("data_encoding", types.Optional(types.TypeUTF8)),
			types.StructField("execution", types.Optional(types.TypeBytes)),
			types.StructField("execution_encoding", types.Optional(types.TypeUTF8)),
			types.StructField("execution_state", types.Optional(types.TypeBytes)),
			types.StructField("execution_state_encoding", types.Optional(types.TypeUTF8)),
			types.StructField("checksum", types.Optional(types.TypeBytes)),
			types.StructField("checksum_encoding", types.Optional(types.TypeUTF8)),
			types.StructField("next_event_id", types.Optional(types.TypeInt64)),
			types.StructField("db_record_version", types.Optional(types.TypeInt64)),
			types.StructField("current_run_id", types.Optional(types.TypeUTF8)),
			types.StructField("last_write_version", types.Optional(types.TypeInt64)),
			types.StructField("state", types.Optional(types.TypeInt32)),
		),
	))
}(upsertRowsQueryID)

var upsertRowsQ = func(id string) string {
	templ := template.Must(template.New("").Parse(`
UPSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_category_id, task_id, event_type, event_id, event_name, task_visibility_ts, data, data_encoding, execution, execution_encoding, execution_state, execution_state_encoding, checksum, checksum_encoding, next_event_id, db_record_version, current_run_id, last_write_version, state)
SELECT shard_id, namespace_id, workflow_id, run_id, task_category_id, task_id, event_type, event_id, event_name, task_visibility_ts, data, data_encoding, execution, execution_encoding, execution_state, execution_state_encoding, checksum, checksum_encoding, next_event_id, db_record_version, current_run_id, last_write_version, state
FROM AS_TABLE($rows_to_add_{{ .qid }}) WHERE $incorrect = 0;
`))
	var buf strings.Builder
	_ = templ.Execute(&buf, map[string]interface{}{
		"qid": id,
	})
	return buf.String()
}(upsertRowsQueryID)

func prepareUpsertRowsQuery(rows []types.Value) *writeQuery {
	return &writeQuery{
		decl:  upsertRowsDecl,
		query: upsertRowsQ,
		params: []table.ParameterOption{
			table.ValueParam("$rows_to_add_"+upsertRowsQueryID, types.ListValue(rows...)),
		},
	}
}

const deleteRowsQueryID = "del_rs"

var deleteRowsDecl = func(id string) string {
	q := xydb.NewQuery()
	q.Declare("shard_id_"+id, types.TypeUint32)
	q.Declare(
		"events_to_delete_"+id,
		types.List(
			types.Struct(
				types.StructField("namespace_id", types.TypeUTF8),
				types.StructField("workflow_id", types.TypeUTF8),
				types.StructField("run_id", types.TypeUTF8),
				types.StructField("event_type", types.TypeInt32),
				types.StructField("event_id", types.Optional(types.TypeInt64)),
				types.StructField("event_name", types.Optional(types.TypeUTF8)),
			),
		),
	)
	return strings.Join(q.Declaration, "\n")
}(deleteRowsQueryID)

var deleteRowsQ = func(id string) string {
	templ := template.Must(template.New("").Parse(`
DELETE FROM executions
ON SELECT
$shard_id_{{ .qid }} as shard_id,
namespace_id,
workflow_id,
run_id,
NULL as task_id,
NULL as task_category_id,
NULL as task_visibility_ts,
event_type,
event_id,
event_name
FROM AS_TABLE($events_to_delete_{{ .qid }})
WHERE $incorrect = 0;
`))
	var buf strings.Builder
	_ = templ.Execute(&buf, map[string]interface{}{
		"qid": id,
	})
	return buf.String()
}(deleteRowsQueryID)

func prepareDeleteEventRowsQuery(shardID int32, eventKeysToRemove []types.Value) *writeQuery {
	return &writeQuery{
		decl:  deleteRowsDecl,
		query: deleteRowsQ,
		params: []table.ParameterOption{
			table.ValueParam("$shard_id_"+deleteRowsQueryID, types.Uint32Value(ToShardIDColumnValue(shardID))),
			table.ValueParam("$events_to_delete_"+deleteRowsQueryID, types.ListValue(eventKeysToRemove...)),
		},
	}
}

const deleteBufferedEventsQueryID = "dbe"

var deleteBufferedEventsDecl = func(id string) string {
	q := xydb.NewQuery()
	q.Declare("shard_id_"+id, types.TypeUint32)
	q.Declare("namespace_id_"+id, types.TypeUTF8)
	q.Declare("workflow_id_"+id, types.TypeUTF8)
	q.Declare("run_id_"+id, types.TypeUTF8)
	q.Declare("buffered_event_type_"+id, types.TypeInt32)
	return strings.Join(q.Declaration, "\n")
}(deleteBufferedEventsQueryID)

var deleteBufferedEventsQ = func(id string) string {
	templ := template.Must(template.New("").Parse(`
DELETE FROM executions
WHERE $incorrect = 0
AND shard_id = $shard_id_{{ .qid }}
AND namespace_id = $namespace_id_{{ .qid }}
AND workflow_id = $workflow_id_{{ .qid }}
AND run_id = $run_id_{{ .qid }}
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type = $buffered_event_type_{{ .qid }};
`))
	var buf strings.Builder
	_ = templ.Execute(&buf, map[string]interface{}{
		"qid": id,
	})
	return buf.String()
}(deleteBufferedEventsQueryID)

func prepareDeleteBufferedEventsQuery(shardID int32, namespaceID, workflowID, runID string) *writeQuery {
	params := []table.ParameterOption{
		table.ValueParam("$shard_id_"+deleteBufferedEventsQueryID, types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$namespace_id_"+deleteBufferedEventsQueryID, types.UTF8Value(namespaceID)),
		table.ValueParam("$workflow_id_"+deleteBufferedEventsQueryID, types.UTF8Value(workflowID)),
		table.ValueParam("$run_id_"+deleteBufferedEventsQueryID, types.UTF8Value(runID)),
		table.ValueParam("$buffered_event_type_"+deleteBufferedEventsQueryID, types.Int32Value(eventTypeBufferedEvent)),
	}
	return &writeQuery{
		decl:   deleteBufferedEventsDecl,
		query:  deleteBufferedEventsQ,
		params: params,
	}
}

const deleteBufferedEvents2QueryID = "dbe2"

var deleteBufferedEvents2Decl = func(id string) string {
	q := xydb.NewQuery()
	q.Declare("shard_id_"+id, types.TypeUint32)
	q.Declare("namespace_id_"+id, types.TypeUTF8)
	q.Declare("workflow_id_"+id, types.TypeUTF8)
	q.Declare("run_id_1_"+id, types.TypeUTF8)
	q.Declare("run_id_2_"+id, types.TypeUTF8)
	q.Declare("buffered_event_type_"+id, types.TypeInt32)
	return strings.Join(q.Declaration, "\n")
}(deleteBufferedEvents2QueryID)

var deleteBufferedEvents2Q = func(id string) string {
	templ := template.Must(template.New("").Parse(`
DELETE FROM executions
WHERE $incorrect = 0 AND ((
    shard_id = $shard_id_{{ .qid }}
	AND namespace_id = $namespace_id_{{ .qid }}
	AND workflow_id = $workflow_id_{{ .qid }}
	AND run_id = $run_id_1_{{ .qid }}
	AND task_id IS NULL
	AND task_category_id IS NULL
	AND task_visibility_ts IS NULL
	AND event_type = $buffered_event_type_{{ .qid }}
) OR (
   	shard_id = $shard_id_{{ .qid }}
	AND namespace_id = $namespace_id_{{ .qid }}
	AND workflow_id = $workflow_id_{{ .qid }}
	AND run_id = $run_id_2_{{ .qid }}
	AND task_id IS NULL
	AND task_category_id IS NULL
	AND task_visibility_ts IS NULL
	AND event_type IS NOT NULL
));
`))
	var buf strings.Builder
	_ = templ.Execute(&buf, map[string]interface{}{
		"qid": id,
	})
	return buf.String()
}(deleteBufferedEvents2QueryID)

func prepareDeleteBufferedEventsForID1AndRemoveAllEventsForID2Query(shardID int32, namespaceID, workflowID, runID1, runID2 string) *writeQuery {
	params := []table.ParameterOption{
		table.ValueParam("$shard_id_"+deleteBufferedEvents2QueryID, types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$namespace_id_"+deleteBufferedEvents2QueryID, types.UTF8Value(namespaceID)),
		table.ValueParam("$workflow_id_"+deleteBufferedEvents2QueryID, types.UTF8Value(workflowID)),
		table.ValueParam("$run_id_1_"+deleteBufferedEvents2QueryID, types.UTF8Value(runID1)),
		table.ValueParam("$run_id_2_"+deleteBufferedEvents2QueryID, types.UTF8Value(runID2)),
		table.ValueParam("$buffered_event_type_"+deleteBufferedEvents2QueryID, types.Int32Value(eventTypeBufferedEvent)),
	}
	return &writeQuery{
		decl:   deleteBufferedEvents2Decl,
		query:  deleteBufferedEvents2Q,
		params: params,
	}
}

const deleteAllEventsQueryID = "dae"

var deleteAllEventsDecl = func(id string) string {
	q := xydb.NewQuery()
	q.Declare("shard_id_"+id, types.TypeUint32)
	q.Declare("namespace_id_"+id, types.TypeUTF8)
	q.Declare("workflow_id_"+id, types.TypeUTF8)
	q.Declare("run_id_"+id, types.TypeUTF8)
	return strings.Join(q.Declaration, "\n")
}(deleteAllEventsQueryID)

var deleteAllEventsQ = func(id string) string {
	templ := template.Must(template.New("").Parse(`
DELETE FROM executions
WHERE $incorrect = 0 AND (
   	shard_id = $shard_id_{{ .qid }}
	AND namespace_id = $namespace_id_{{ .qid }}
	AND workflow_id = $workflow_id_{{ .qid }}
	AND run_id = $run_id_{{ .qid }}
	AND task_id IS NULL
	AND task_category_id IS NULL
	AND task_visibility_ts IS NULL
	AND event_type IS NOT NULL
);
`))
	var buf strings.Builder
	_ = templ.Execute(&buf, map[string]interface{}{
		"qid": id,
	})
	return buf.String()
}(deleteAllEventsQueryID)

func prepareRemoveAllEventsForIDQuery(shardID int32, namespaceID, workflowID, runID string) *writeQuery {
	params := []table.ParameterOption{
		table.ValueParam("$shard_id_"+deleteAllEventsQueryID, types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$namespace_id_"+deleteAllEventsQueryID, types.UTF8Value(namespaceID)),
		table.ValueParam("$workflow_id_"+deleteAllEventsQueryID, types.UTF8Value(workflowID)),
		table.ValueParam("$run_id_"+deleteAllEventsQueryID, types.UTF8Value(runID)),
	}
	return &writeQuery{
		decl:   deleteAllEventsDecl,
		query:  deleteAllEventsQ,
		params: params,
	}
}
