package conn

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.temporal.io/server/common/primitives"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
)

type Query struct {
	Values        map[string]types.Value
	QueryTemplate string
}

func NewQueryPart(values map[string]types.Value, queryTemplate string) *Query {
	return &Query{
		Values:        values,
		QueryTemplate: queryTemplate,
	}
}

func NewSingleArgQueryPart(argName string, v types.Value, queryTemplate string) *Query {
	return &Query{
		Values:        map[string]types.Value{argName: v},
		QueryTemplate: queryTemplate,
	}
}

type Stmt struct {
	Declaration []string
	Queries     []string
	Params      []table.ParameterOption
}

func (b *Query) ToStmt(prefix string, conditionID primitives.UUID) *Stmt {
	var rv Stmt
	rv.Queries = append(rv.Queries, fmt.Sprintf(b.QueryTemplate, prefix))

	for name, p := range b.Values {
		name = prefix + strings.TrimPrefix(name, "$")
		rv.Params = append(rv.Params, table.ValueParam("$"+name, p))
	}

	decl, err := sugar.GenerateDeclareSection(rv.Params)
	if err != nil {
		panic(err)
	}
	rv.Declaration = append(rv.Declaration, decl)

	return &rv
}

var _ executor.Query[*Stmt] = (*Query)(nil)
