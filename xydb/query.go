package xydb

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type QueryConstructor struct {
	Declaration []string
	Queries     []string
}

func NewQuery() *QueryConstructor {
	return &QueryConstructor{}
}

func (b *QueryConstructor) Declare(name string, t types.Type) {
	name = strings.TrimPrefix(name, "$")
	b.Declaration = append(b.Declaration, fmt.Sprintf("DECLARE $%s AS %s;", name, t))
}

func (b *QueryConstructor) AddQuery(query string) {
	b.Queries = append(b.Queries, query)
}
