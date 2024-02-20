package persistencelite

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	ydbp "github.com/yandex/temporal-over-ydb/persistence"
	"github.com/yandex/temporal-over-ydb/xydb"
)

type YDBLiteConfig struct {
	RqliteConnURL string
	RqliteConnN   int
	ShardCount    int32
	xydb.Config
}

func OptionsToYDBLiteConfig(options map[string]any) (YDBLiteConfig, error) {
	res := YDBLiteConfig{}
	var err error
	res.Config, err = ydbp.OptionsToYDBConfig(options)
	if err != nil {
		return res, fmt.Errorf("failed to read YDB config: %w", err)
	}
	if shardCount, ok := options["shard_count"]; ok {
		switch v := shardCount.(type) {
		case int:
			res.ShardCount = int32(v)
		case int32:
			res.ShardCount = v
		case int64:
			res.ShardCount = int32(v)
		case float64:
			res.ShardCount = int32(v)
		case float32:
			res.ShardCount = int32(v)
		default:
			return res, errors.New("failed to set: shard_count: " + reflect.TypeOf(v).String())
		}
	} else {
		return res, errors.New("missing required option: shard_count")
	}
	if rqliteConnURL, ok := options["rqlite_conn_url"].(string); ok {
		res.RqliteConnURL = rqliteConnURL
	} else {
		return res, errors.New("missing required option: rqlite_conn_url")
	}
	if rqliteConnN, ok := options["rqlite_conn_n"].(string); ok {
		res.RqliteConnN, err = strconv.Atoi(rqliteConnN)
		if err != nil {
			return res, fmt.Errorf("failed to set rqlite_conn_n: %w", err)
		}
	}
	return res, nil
}
