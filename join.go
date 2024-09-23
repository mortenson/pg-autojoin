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

type JoinBehavior string

var (
	JoinBehaviorLeftJoin  JoinBehavior = "JoinBehaviorLeftJoin"
	JoinBehaviorInnerJoin JoinBehavior = "JoinBehaviorInnerJoin"
)

// Useful information for telling the end user what happened during the join.
type MissingJoinResult struct {
	MissingColumnsToJoinedTables   map[string]string
	MissingColumnsToPossibleTables map[string]map[string]string
}

func addMissingJoinsToSelect(stmt *pg_query.RawStmt, databaseInfo DatabaseInfo, joinBehavior JoinBehavior) (MissingJoinResult, error) {
	joinPlan := MissingJoinResult{
		MissingColumnsToJoinedTables:   map[string]string{},
		MissingColumnsToPossibleTables: map[string]map[string]string{},
	}

	// Parse the query.
	query := TraverseQuery(stmt, 0)

	// Set up some helpful maps for later.
	queryTableNames := map[string]string{}
	originalQueryTableNames := map[string]string{}
	for _, table := range query.Tables {
		queryTableNames[table.Name] = table.Name
		originalQueryTableNames[table.Name] = table.Name
	}

	tableToAlias := map[string]string{}
	aliasToTable := map[string]string{}
	for _, table := range query.Tables {
		if table.Alias != nil {
			tableToAlias[table.Name] = *table.Alias
			aliasToTable[*table.Alias] = table.Name
		}
	}
	aliasTable := func(tableName string) string {
		aliasName, ok := tableToAlias[tableName]
		if ok {
			return aliasName
		}
		return tableName
	}
	unAliasTable := func(aliasName string) string {
		tableName, ok := aliasToTable[aliasName]
		if ok {
			return tableName
		}
		return aliasName
	}

	allPaths := [][]string{}
	queryColumnsSorted := slices.Sorted(maps.Keys(query.Columns))
	for _, columnKey := range queryColumnsSorted {
		var tablesThatHaveColumn []string
		column := query.Columns[columnKey]

		// Get the table name from the column alias, if possible.
		var aliasTableName *string
		if column.Alias != nil {
			aliasRef := unAliasTable(*column.Alias)
			table, tableInQuery := queryTableNames[aliasRef]
			// Alias is likely coming from an existing FROM/JOIN.
			if tableInQuery {
				// We should still prefix this column since it came from a new join.
				_, tableInOriginalQuery := originalQueryTableNames[table]
				if !tableInOriginalQuery {
					joinPlan.MissingColumnsToJoinedTables[column.Name] = table
				}
				continue
			}
			aliasTableName = &aliasRef
		}

		// For wildcards like foo.*, assume the user wants the "foo" table.
		if column.Type == QueryColumnTypeTableWildcard {
			tablesThatHaveColumn = []string{*aliasTableName}
		} else {
			matches, ok := databaseInfo.ColumnToTable[column.Name]
			if !ok {
				return joinPlan, fmt.Errorf("could not find table with column %s, maybe the database schema changed?", column.Name)
			}
			tablesThatHaveColumn = slices.Clone(matches)
			slices.Sort(tablesThatHaveColumn)
		}

		if column.Type == QueryColumnTypeAliasedColumn && slices.Contains(tablesThatHaveColumn, *aliasTableName) {
			slog.Debug(fmt.Sprintf("Using alias to imply join for %s", column))
			tablesThatHaveColumn = []string{*aliasTableName}
		}

		// See if the column already exists in a table in the query, if so we can ignore.
		columnExistsInQuery := false
		for _, table := range tablesThatHaveColumn {
			_, tableInQuery := queryTableNames[table]
			if tableInQuery {
				columnExistsInQuery = true
				// We should still prefix this column since it came from a new join.
				_, tableInOriginalQuery := originalQueryTableNames[table]
				if !tableInOriginalQuery {
					joinPlan.MissingColumnsToJoinedTables[column.Name] = table
				}
				break
			}
		}
		if columnExistsInQuery {
			continue
		}

		// We need to join. Find the shortest path from a table that has the column to a table that exists in the query.
		shortestPath := []string{}
		queryTableNamesSorted := slices.Sorted(maps.Keys(queryTableNames))
		for _, otherTableName := range tablesThatHaveColumn {
			for _, queryTableName := range queryTableNamesSorted {
				path, _ := graph.ShortestPath(databaseInfo.RelationshipGraph, queryTableName, otherTableName)
				if len(path) == 0 {
					continue
				}

				// Add debug output to join plan.
				_, ok := joinPlan.MissingColumnsToPossibleTables[column.Name]
				if !ok {
					joinPlan.MissingColumnsToPossibleTables[column.Name] = map[string]string{}
				}
				joinPlan.MissingColumnsToPossibleTables[column.Name][otherTableName] = otherTableName

				_, isOriginalQueryTable := originalQueryTableNames[queryTableName]
				if len(shortestPath) == 0 ||
					len(path) < len(shortestPath) ||
					// Break ties if the path is coming from a table the user had in their original query.
					(len(path) == len(shortestPath) && isOriginalQueryTable) {
					shortestPath = path
				}
			}
		}
		if len(shortestPath) == 0 {
			slog.Debug(fmt.Sprintf("Cannot find shortest path for %s", column))
			continue
		} else {
			slog.Debug(fmt.Sprintf("Shortest path for %s is %s", column, strings.Join(shortestPath, ", ")))
			allPaths = append(allPaths, shortestPath)
			joinPlan.MissingColumnsToJoinedTables[column.Name] = shortestPath[len(shortestPath)-1]
			// Update queryTableNames so that sub-paths (JOINs) are never duplicated.
			for _, pathTableName := range shortestPath {
				queryTableNames[pathTableName] = pathTableName
			}
		}
	}

	// Add joins to the parsed query.
	for _, path := range allPaths {
		lastTable := ""
		for _, tableName := range path {
			if lastTable == "" {
				lastTable = tableName
				continue
			}
			// See what direction we need to join.
			// @todo this could probably be stored in the graph, then allPaths would be vertexes not names.
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
				return joinPlan, fmt.Errorf("could not find matching foreign key for %s <=> %s", lastTable, tableName)
			}

			// It's much easier parse a dummy query into an AST than constructing an AST ourselves.
			// If this is extremely unperformant we can construct an AST, maybe from JSON/protobuf.
			var joinStr string
			if joinBehavior == JoinBehaviorInnerJoin {
				joinStr = "JOIN"
			} else {
				joinStr = "LEFT JOIN"
			}
			joinQuery := fmt.Sprintf("select placeholder FROM foo %s %s ON ", joinStr, aliasTable(tableName))
			conditions := []string{}
			for _, fromToPair := range matchingFkey.ColumnConditions {
				conditions = append(conditions, fmt.Sprintf("%s.%s = %s.%s", aliasTable(fromTable), fromToPair[0], aliasTable(matchingFkey.ToTable), fromToPair[1]))
			}
			joinQuery += strings.Join(conditions, " AND ")
			joinParsed, err := pg_query.Parse(joinQuery)
			if err != nil {
				return joinPlan, err
			}
			// Wrap existing from clause with the new join.
			joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0].GetJoinExpr().Larg = stmt.Stmt.GetSelectStmt().FromClause[0]
			// Replace existing from clause with wrapped from clause.
			stmt.Stmt.GetSelectStmt().FromClause[0] = joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
			// The next join will be from this table.
			lastTable = tableName
		}
	}

	return joinPlan, nil
}

// Attempts to add JOINs to queries that reference columns from other tables.
func AddMissingJoinsToQuery(parsedQuery *pg_query.ParseResult, databaseInfo DatabaseInfo, joinBehavior JoinBehavior) (MissingJoinResult, error) {
	var joinPlan MissingJoinResult
	for _, stmt := range parsedQuery.GetStmts() {
		// We can only safely do this on SELECTs.
		if stmt.Stmt.GetSelectStmt() == nil {
			continue
		}
		tableMap, err := addMissingJoinsToSelect(stmt, databaseInfo, joinBehavior)
		if err != nil {
			return joinPlan, err
		}
		joinPlan = tableMap
	}
	return joinPlan, nil
}
