package pg_autojoin

import (
	"context"

	"github.com/dominikbraun/graph"
	"github.com/jackc/pgx/v5"
)

const columnsWithForeignKeysQuery = `
select col.table_name as table,
       col.column_name,
       COALESCE(rel.table_name, '') as primary_table,
       COALESCE(rel.column_name, '') as primary_column,
			 COALESCE(kcu.constraint_name, '')
from information_schema.columns col
left join (select kcu.constraint_schema, 
                  kcu.constraint_name, 
                  kcu.table_schema,
                  kcu.table_name, 
                  kcu.column_name, 
                  kcu.ordinal_position,
                  kcu.position_in_unique_constraint
           from information_schema.key_column_usage kcu
           join information_schema.table_constraints tco
                on kcu.constraint_schema = tco.constraint_schema
                and kcu.constraint_name = tco.constraint_name
                and tco.constraint_type = 'FOREIGN KEY'
          ) as kcu
          on col.table_schema = kcu.table_schema
          and col.table_name = kcu.table_name
          and col.column_name = kcu.column_name
left join information_schema.referential_constraints rco
          on rco.constraint_name = kcu.constraint_name
          and rco.constraint_schema = kcu.table_schema
left join information_schema.key_column_usage rel
          on rco.unique_constraint_name = rel.constraint_name
          and rco.unique_constraint_schema = rel.constraint_schema
          and rel.ordinal_position = kcu.position_in_unique_constraint
where col.table_schema = 'public';
`

type ForeignKey struct {
	ToTable          string
	ColumnConditions [][2]string
}

type TableInfo struct {
	Name    string
	Columns []string
	// Constraint -> Fkey
	ForeignKeys map[string]*ForeignKey
}

type DatabaseInfo struct {
	Tables            map[string]*TableInfo
	ColumnToTable     map[string][]string
	RelationshipGraph graph.Graph[string, string]
}

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Gathers information about every table's column and foreign key.
func GetDatabaseInfoResult(ctx context.Context, conn Queryer) (DatabaseInfo, error) {
	rows, err := conn.Query(ctx, columnsWithForeignKeysQuery)
	if err != nil {
		return DatabaseInfo{}, err
	}
	defer rows.Close()

	tableInfo := map[string]*TableInfo{}

	for rows.Next() {
		var fromTableName string
		var fromColumnName string
		var toTableName string
		var toColumnName string // Unused
		var constaintName string
		err = rows.Scan(&fromTableName, &fromColumnName, &toTableName, &toColumnName, &constaintName)
		if err != nil {
			return DatabaseInfo{}, err
		}
		_, tableExists := tableInfo[fromTableName]
		if !tableExists {
			tableInfo[fromTableName] = &TableInfo{
				Name:        fromTableName,
				Columns:     []string{},
				ForeignKeys: map[string]*ForeignKey{},
			}
		}
		tableInfo[fromTableName].Columns = append(tableInfo[fromTableName].Columns, fromColumnName)
		if constaintName != "" {
			_, fkeyExists := tableInfo[fromTableName].ForeignKeys[constaintName]
			if !fkeyExists {
				tableInfo[fromTableName].ForeignKeys[constaintName] = &ForeignKey{
					ToTable:          toTableName,
					ColumnConditions: [][2]string{},
				}
			}
			if fromColumnName != "" && toColumnName != "" {
				tableInfo[fromTableName].ForeignKeys[constaintName].ColumnConditions = append(
					tableInfo[fromTableName].ForeignKeys[constaintName].ColumnConditions,
					[2]string{fromColumnName, toColumnName},
				)
			}
		}
	}

	// Add all tables to a graph.
	relationshipGraph := graph.New(graph.StringHash)
	for tableName := range tableInfo {
		err = relationshipGraph.AddVertex(tableName)
		if err != nil {
			return DatabaseInfo{}, err
		}
	}
	for tableName, table := range tableInfo {
		for _, fkey := range table.ForeignKeys {
			err = relationshipGraph.AddEdge(tableName, fkey.ToTable)
			if err != nil {
				return DatabaseInfo{}, err
			}
		}
	}

	// Create a map of column name to table name for future use.
	columnToTable := map[string][]string{}
	for tableName, table := range tableInfo {
		for _, column := range table.Columns {
			_, ok := columnToTable[column]
			if !ok {
				columnToTable[column] = []string{}
			}
			columnToTable[column] = append(columnToTable[column], tableName)
		}
	}

	return DatabaseInfo{tableInfo, columnToTable, relationshipGraph}, nil
}
