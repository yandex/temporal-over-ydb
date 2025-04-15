package log

import (
	"context"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	tlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
)

var _ log.Logger = adapter{}

type adapter struct {
	l tlog.Logger
}

func (a adapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	tags := Tags(fields)
	tags = append(tags, tag.NewStringTag("namespace", strings.Join(log.NamesFromContext(ctx), ".")))

	switch log.LevelFromContext(ctx) {
	case log.TRACE, log.DEBUG:
		a.l.Debug(msg, tags...)
	case log.INFO:
		a.l.Info(msg, tags...)
	case log.WARN:
		a.l.Warn(msg, tags...)
	case log.ERROR:
		a.l.Error(msg, tags...)
	case log.FATAL:
		a.l.Fatal(msg, tags...)
	default:
		a.l.Error("[Unknown log level] "+msg, tags...)
	}
}

func fieldToField(field log.Field) tag.Tag {
	var f zap.Field
	switch field.Type() {
	case log.IntType:
		f = zap.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		f = zap.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		f = zap.String(field.Key(), field.StringValue())
	case log.BoolType:
		f = zap.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		f = zap.Duration(field.Key(), field.DurationValue())
	case log.StringsType:
		f = zap.Strings(field.Key(), field.StringsValue())
	case log.ErrorType:
		f = zap.Error(field.ErrorValue())
	case log.StringerType:
		f = zap.Stringer(field.Key(), field.Stringer())
	default:
		f = zap.Any(field.Key(), field.AnyValue())
	}
	return tag.NewZapTag(f)
}

func Tags(fields []log.Field) []tag.Tag {
	tags := make([]tag.Tag, len(fields))
	for i, f := range fields {
		tags[i] = fieldToField(f)
	}
	return tags
}
