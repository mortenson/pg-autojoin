package pg_autojoin

import (
	"log/slog"
	"reflect"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type QueryColumnType string

var (
	QueryColumnTypeTableWildcard QueryColumnType = "ColumnTypeTableWildcard"
	QueryColumnTypeColumn        QueryColumnType = "ColumnTypeColumn"
	QueryColumnTypeAliasedColumn QueryColumnType = "QueryColumnTypeAliasedColumn"
)

type QueryColumn struct {
	Type  QueryColumnType
	Name  string
	Alias *string
}

func (qc QueryColumn) String() string {
	if qc.Alias != nil {
		return *qc.Alias + "." + qc.Name
	}
	return qc.Name
}

type QueryTable struct {
	Name  string
	Alias *string
}

type Query struct {
	Columns map[string]QueryColumn
	Tables  map[string]QueryTable
}

func getColumnsFromRef(ref *pg_query.ColumnRef) []QueryColumn {
	svals := []string{}
	isWildcard := false
	for _, field := range ref.Fields {
		if field.GetString_() != nil {
			svals = append(svals, field.GetString_().Sval)
		} else if field.GetAStar() != nil {
			isWildcard = true
		}
	}
	if len(svals) == 1 {
		if len(ref.Fields) == 2 && isWildcard {
			return []QueryColumn{{QueryColumnTypeTableWildcard, "*", &svals[0]}}
		} else if len(ref.Fields) == 1 {
			return []QueryColumn{{QueryColumnTypeColumn, svals[0], nil}}
		}
	} else if len(svals) == 2 {
		return []QueryColumn{{QueryColumnTypeAliasedColumn, svals[1], &svals[0]}}
	} else {
		slog.Debug("Could not determine type of column ref", slog.Any("columnRef", ref))
	}
	return []QueryColumn{}
}

func mergeQuery(a, b Query) Query {
	for name, col := range b.Columns {
		a.Columns[name] = col
	}
	for name, table := range b.Tables {
		a.Tables[name] = table
	}
	return a
}

// Taken from https://github.com/pganalyze/pg_query_go/issues/18#issuecomment-475632691
// Traverses the given query AST and pulls all table/column references out of it.
func TraverseQuery(value interface{}, depth int) Query {
	query := Query{
		Columns: map[string]QueryColumn{},
		Tables:  map[string]QueryTable{},
	}

	if value == nil {
		return Query{
			Columns: map[string]QueryColumn{},
			Tables:  map[string]QueryTable{},
		}
	}

	t := reflect.TypeOf(value)
	v := reflect.ValueOf(value)

	if v.Type() == reflect.TypeOf(pg_query.RangeVar{}) {
		var alias *string
		if value.(pg_query.RangeVar).Alias != nil {
			alias = &value.(pg_query.RangeVar).Alias.Aliasname
		}
		query.Tables[value.(pg_query.RangeVar).Relname] = QueryTable{value.(pg_query.RangeVar).Relname, alias}
	}

	if v.Type() == reflect.TypeOf(pg_query.ColumnRef{}) {
		columnRef := pg_query.ColumnRef{
			Fields: value.(pg_query.ColumnRef).Fields,
		}
		for _, col := range getColumnsFromRef(&columnRef) {
			query.Columns[col.String()] = col
		}
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
