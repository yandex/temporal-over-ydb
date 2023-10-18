package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	tlog "go.temporal.io/server/common/log"
)

func WithTraces(l tlog.Logger, d trace.Detailer, opts ...log.Option) ydb.Option {
	a := adapter{l: l}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(log.Driver(a, d, opts...)),
		ydb.WithTraceTable(log.Table(a, d, opts...)),
		ydb.WithTraceScripting(log.Scripting(a, d, opts...)),
		ydb.WithTraceScheme(log.Scheme(a, d, opts...)),
		ydb.WithTraceCoordination(log.Coordination(a, d, opts...)),
		ydb.WithTraceRatelimiter(log.Ratelimiter(a, d, opts...)),
		ydb.WithTraceDiscovery(log.Discovery(a, d, opts...)),
		ydb.WithTraceTopic(log.Topic(a, d, opts...)),
		ydb.WithTraceDatabaseSQL(log.DatabaseSQL(a, d, opts...)),
	)
}

func WithLogger(l tlog.Logger, d trace.Detailer, opts ...log.Option) ydb.Option {
	return ydb.WithLogger(adapter{l: l}, d, opts...)
}

func Table(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Table {
	return log.Table(&adapter{l: l}, d, opts...)
}

func Topic(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Topic {
	return log.Topic(&adapter{l: l}, d, opts...)
}

func Driver(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Driver {
	return log.Driver(&adapter{l: l}, d, opts...)
}

func Coordination(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Coordination {
	return log.Coordination(&adapter{l: l}, d, opts...)
}

func Discovery(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Discovery {
	return log.Discovery(&adapter{l: l}, d, opts...)
}

func Ratelimiter(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Ratelimiter {
	return log.Ratelimiter(&adapter{l: l}, d, opts...)
}

func Scheme(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Scheme {
	return log.Scheme(&adapter{l: l}, d, opts...)
}

func Scripting(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.Scripting {
	return log.Scripting(&adapter{l: l}, d, opts...)
}

func DatabaseSQL(l tlog.Logger, d trace.Detailer, opts ...log.Option) trace.DatabaseSQL {
	return log.DatabaseSQL(&adapter{l: l}, d, opts...)
}
