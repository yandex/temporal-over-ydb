package conn

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rqlite/gorqlite"
	"go.temporal.io/server/common/metrics"
)

type ThreadSafeRqliteConnection interface {
	RequestParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.RequestResult, err error)
	WriteParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.WriteResult, err error)
	QueryParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.QueryResult, err error)
}

// ToSQLiteDateTime converts to time to SQLite datetime
func ToSQLiteDateTime(t time.Time) int64 { //time.Time {
	if t.IsZero() {
		return 0
	}
	return t.UTC().Truncate(time.Microsecond).UnixMicro()
}

// FromSQLiteDateTime converts SQLite datetime and returns go time
func FromSQLiteDateTime(t int64) time.Time {
	return time.UnixMicro(t).UTC()
}

type RqliteFactory interface {
	GetClient(shardID int32) ThreadSafeRqliteConnection
	ListConns() []ThreadSafeRqliteConnection
}

type threadSafeRqliteConnectionImpl struct {
	m       sync.Mutex
	metrics metrics.Handler
	conn    *gorqlite.Connection
}

type rqliteFactoryImpl struct {
	conns [][]ThreadSafeRqliteConnection
}

func (f *rqliteFactoryImpl) GetClient(shardID int32) ThreadSafeRqliteConnection {
	i := rand.Intn(len(f.conns))
	cs := f.conns[i]
	j := int(shardID) % len(cs)
	return cs[j]
}

func (f *rqliteFactoryImpl) ListConns() []ThreadSafeRqliteConnection {
	return f.conns[0]
}

func (c *threadSafeRqliteConnectionImpl) RequestParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.RequestResult, err error) {
	begin := time.Now()
	c.m.Lock()
	c.metrics.Timer("rqlite_request_lock_latency").Record(time.Since(begin))
	defer c.m.Unlock()
	defer c.metrics.Timer("rqlite_request_latency").Record(time.Since(begin))
	rs, err := c.conn.RequestParameterizedContext(ctx, sqlStatements)

	if err == nil {
		for i, r := range rs {
			if r.Err != nil {
				fmt.Printf("XXX oh wow, err == nil but r.Err != nil, i = " + fmt.Sprint(i) + " " + r.Err.Error() + "\n")
				os.Exit(1)
			}
		}
	}

	return rs, err
}

func (c *threadSafeRqliteConnectionImpl) WriteParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.WriteResult, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	rs, err := c.conn.WriteParameterizedContext(ctx, sqlStatements)

	if err == nil {
		for i, r := range rs {
			if r.Err != nil {
				fmt.Printf("XXX oh wow, err == nil but r.Err != nil, i = " + fmt.Sprint(i) + " " + r.Err.Error() + "\n")
				os.Exit(1)
			}
		}
	}

	return rs, err
}

func (c *threadSafeRqliteConnectionImpl) QueryParameterizedContext(ctx context.Context, sqlStatements []gorqlite.ParameterizedStatement) (results []gorqlite.QueryResult, err error) {
	begin := time.Now()
	c.m.Lock()
	c.metrics.Timer("rqlite_query_lock_latency").Record(time.Since(begin))
	defer c.m.Unlock()
	defer c.metrics.Timer("rqlite_query_latency").Record(time.Since(begin))
	rs, err := c.conn.QueryParameterizedContext(ctx, sqlStatements)

	if err == nil {
		for i, r := range rs {
			if r.Err != nil {
				fmt.Printf("XXX oh wow, err == nil but r.Err != nil, i = " + fmt.Sprint(i) + " " + r.Err.Error() + "\n")
				os.Exit(1)
			}
		}
	}

	return rs, err
}

func NewThreadSafeRqliteConnection(metrics metrics.Handler, conn *gorqlite.Connection) ThreadSafeRqliteConnection {
	return &threadSafeRqliteConnectionImpl{
		metrics: metrics,
		conn:    conn,
	}
}

func NewRqliteFactoryFromConnURL(metrics metrics.Handler, connURLs string, n int) (RqliteFactory, error) {
	rqliteConnURLs := strings.Split(connURLs, ",")
	rqliteConns := make([][]ThreadSafeRqliteConnection, 0, n)
	for i := 0; i < n; i++ {
		conns := make([]ThreadSafeRqliteConnection, 0, len(connURLs))
		for _, rqliteConnURL := range rqliteConnURLs {
			rqliteClient, err := gorqlite.Open(rqliteConnURL)
			if err != nil {
				return nil, fmt.Errorf("failed to create rqlite client for %s: %w", rqliteConnURL, err)
			}
			_ = rqliteClient.SetExecutionWithTransaction(true)
			_ = rqliteClient.SetConsistencyLevel(gorqlite.ConsistencyLevelStrong)
			conns = append(conns, NewThreadSafeRqliteConnection(metrics, rqliteClient))
		}
		rqliteConns = append(rqliteConns, conns)
	}
	return &rqliteFactoryImpl{
		conns: rqliteConns,
	}, nil
}
