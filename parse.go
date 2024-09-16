package pg_autojoin

import (
	"reflect"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type QueryColumnType string

var (
	QueryColumnTypeTableWildcard QueryColumnType = "ColumnTypeTableWildcard"
	QueryColumnTypeColumn        QueryColumnType = "ColumnTypeColumn"
)

type QueryColumn struct {
	Type  QueryColumnType
	Value string
}

type QueryTable struct {
	Value string
}

type Query struct {
	Columns []QueryColumn
	Tables  []QueryTable
	Aliases map[string]QueryTable
}

func getColumnsFromRef(ref *pg_query.ColumnRef) []QueryColumn {
	var fieldName string
	isWildcard := false
	for _, field := range ref.Fields {
		if field.GetString_() != nil {
			fieldName = field.GetString_().Sval
		} else if field.GetAStar() != nil {
			isWildcard = true
		}
	}
	if len(ref.Fields) == 2 && isWildcard {
		return []QueryColumn{{QueryColumnTypeTableWildcard, fieldName}}
	} else if len(ref.Fields) == 1 && fieldName != "" {
		return []QueryColumn{{QueryColumnTypeColumn, fieldName}}
	}
	return []QueryColumn{}
}

func mergeQuery(a, b Query) Query {
	return Query{
		Columns: append(a.Columns, b.Columns...),
		Tables:  append(a.Tables, b.Tables...),
	}
}

// Taken from https://github.com/pganalyze/pg_query_go/issues/18#issuecomment-475632691
// Traverses the given query AST and pulls all table/column references out of it.
func TraverseQuery(value interface{}, depth int) Query {
	query := Query{
		Columns: []QueryColumn{},
		Tables:  []QueryTable{},
	}

	if value == nil {
		return Query{
			Columns: []QueryColumn{},
			Tables:  []QueryTable{},
		}
	}

	t := reflect.TypeOf(value)
	v := reflect.ValueOf(value)

	if v.Type() == reflect.TypeOf(pg_query.RangeVar{}) {
		query.Tables = append(query.Tables, QueryTable{value.(pg_query.RangeVar).Relname})
	}

	if v.Type() == reflect.TypeOf(pg_query.ColumnRef{}) {
		columnRef := pg_query.ColumnRef{
			Fields: value.(pg_query.ColumnRef).Fields,
		}
		query.Columns = append(query.Columns, getColumnsFromRef(&columnRef)...)
	}

	switch t.Kind() {
	case reflect.Ptr:
		if v.Elem().IsValid() {
			query = mergeQuery(query, TraverseQuery(v.Elem().Interface(), depth+1))
		}
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
		depth--
		if v.Len() > 0 {
			for i := 0; i < v.Len(); i++ {
				depth++
				query = mergeQuery(query, TraverseQuery(v.Index(i).Interface(), depth+1))
				depth--
			}
		}
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			query = mergeQuery(query, TraverseQuery(reflect.ValueOf(value).Field(i).Interface(), depth+1))
		}
	}
	return query
}
