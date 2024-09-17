package pg_autojoin

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/dominikbraun/graph"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func AddMissingJoinsToSelect(stmt *pg_query.RawStmt, databaseInfo DatabaseInfo) error {
	// Parse the query.
	query := TraverseQuery(stmt, 0)
	queryTableNames := map[string]string{}
	for _, table := range query.Tables {
		queryTableNames[table.Name] = table.Name
	}
	allPaths := [][]string{}
	// Go maps do not respect insert order, so to provide consistent experience for users sort them alphanumerically.
	queryColumnsSorted := slices.Sorted(maps.Keys(query.Columns))
	for _, columnKey := range queryColumnsSorted {
		column := query.Columns[columnKey]
		// @todo add arbitrary join for this case, or any aliases to tables that don't exist.
		if column.Type == QueryColumnTypeTableWildcard {
			continue
		}
		tablesThatHaveColumn, ok := databaseInfo.ColumnToTable[column.Name]
		if !ok {
			return fmt.Errorf("could not find table with column %s, maybe the database schema changed?", column.Name)
		}
		// For more consistent behavior.
		slices.Sort(tablesThatHaveColumn)

		// Users can explicitly say what table they want using aliases.
		if column.Alias != nil && slices.Contains(tablesThatHaveColumn, *column.Alias) {
			slog.Debug(fmt.Sprintf("Using alias to imply join for %s.%s", *column.Alias, column.Name))
			tablesThatHaveColumn = []string{*column.Alias}
		}

		columnExistsInQuery := false
		for _, table := range tablesThatHaveColumn {
			_, tableInQuery := queryTableNames[table]
			if tableInQuery {
				columnExistsInQuery = true
				break
			}
		}
		if columnExistsInQuery {
			continue
		}
		shortestPath := []string{}
		for _, otherTableName := range tablesThatHaveColumn {
			for _, queryTableName := range queryTableNames {
				path, _ := graph.ShortestPath(databaseInfo.RelationshipGraph, queryTableName, otherTableName)
				// The graph is directed so need to explicitly try the other direction.
				if len(path) == 0 {
					path, _ = graph.ShortestPath(databaseInfo.RelationshipGraph, otherTableName, queryTableName)
					slices.Reverse(path)
				}
				if len(path) == 0 {
					continue
				}
				if len(shortestPath) == 0 || len(path) <= len(shortestPath) {
					shortestPath = path
				}
			}
		}
		if len(shortestPath) == 0 {
			slog.Debug(fmt.Sprintf("Cannot find shortest path for %s", column.Name))
			continue
		} else {
			slog.Debug(fmt.Sprintf("Shortest path for %s is %s", column.Name, strings.Join(shortestPath, ", ")))
			allPaths = append(allPaths, shortestPath)
			for _, pathTableName := range shortestPath {
				queryTableNames[pathTableName] = pathTableName
			}
		}
	}

	// Aliases are slightly tricky, easier to just always use the alias instead of real table name.
	tableToAlias := map[string]string{}
	for _, table := range query.Tables {
		if table.Alias != nil {
			tableToAlias[table.Name] = *table.Alias
		}
	}
	aliasTable := func(tableName string) string {
		aliasName, ok := tableToAlias[tableName]
		if ok {
			return aliasName
		}
		return tableName
	}

	for _, path := range allPaths {
		lastTable := ""
		for _, tableName := range path {
			if lastTable == "" {
				lastTable = tableName
				continue
			}
			// Add a join.
			// It's much easier to construct a AST from a string than constructing one ourselves.
			var fromTable string
			var matchingFkey *ForeignKey
			for _, fkey := range databaseInfo.Tables[lastTable].ForeignKeys {
				if fkey.ToTable == tableName {
					matchingFkey = fkey
					fromTable = lastTable
					break
				}
			}
			if matchingFkey == nil {
				for _, fkey := range databaseInfo.Tables[tableName].ForeignKeys {
					if fkey.ToTable == lastTable {
						matchingFkey = fkey
						fromTable = tableName
						break
					}
				}
			}
			if matchingFkey == nil {
				return fmt.Errorf("could not find matching foreign key for %s <=> %s", lastTable, tableName)
			}

			joinQuery := "select placeholder FROM foo LEFT JOIN " + aliasTable(tableName) + " ON "
			conditions := []string{}
			for _, fromToPair := range matchingFkey.ColumnConditions {
				conditions = append(conditions, fmt.Sprintf("%s.%s = %s.%s", aliasTable(fromTable), fromToPair[0], aliasTable(matchingFkey.ToTable), fromToPair[1]))
			}
			joinQuery += strings.Join(conditions, " AND ")
			joinParsed, err := pg_query.Parse(joinQuery)
			if err != nil {
				return err
			}
			// Wrap existing from clause with the new join.
			joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0].GetJoinExpr().Larg = stmt.Stmt.GetSelectStmt().FromClause[0]
			// Replace existing from clause with wrapped from clause.
			stmt.Stmt.GetSelectStmt().FromClause[0] = joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
			// The next join will be from this table.
			lastTable = tableName
		}
	}

	return nil
}
