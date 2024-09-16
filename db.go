package pg_autojoin

import (
	"context"

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
	Columns []string
	// Constraint -> Fkey
	ForeignKeys map[string]*ForeignKey
}

func GetTableInfoResult(ctx context.Context, tx pgx.Tx) (map[string]*TableInfo, error) {
	rows, err := tx.Query(ctx, columnsWithForeignKeysQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]*TableInfo{}

	for rows.Next() {
		var fromTableName string
		var fromColumnName string
		var toTableName string
		var toColumnName string // Unused
		var constaintName string
		err = rows.Scan(&fromTableName, &fromColumnName, &toTableName, &toColumnName, &constaintName)
		if err != nil {
			return nil, err
		}
		_, tableExists := result[fromTableName]
		if !tableExists {
			result[fromTableName] = &TableInfo{
				Columns:     []string{},
				ForeignKeys: map[string]*ForeignKey{},
			}
		}
		result[fromTableName].Columns = append(result[fromTableName].Columns, fromColumnName)
		if constaintName != "" {
			_, fkeyExists := result[fromTableName].ForeignKeys[constaintName]
			if !fkeyExists {
				result[fromTableName].ForeignKeys[constaintName] = &ForeignKey{
					ToTable:          toTableName,
					ColumnConditions: [][2]string{},
				}
			}
			if fromColumnName != "" && toColumnName != "" {
				result[fromTableName].ForeignKeys[constaintName].ColumnConditions = append(
					result[fromTableName].ForeignKeys[constaintName].ColumnConditions,
					[2]string{fromColumnName, toColumnName},
				)
			}
		}
	}
	return result, nil
}
