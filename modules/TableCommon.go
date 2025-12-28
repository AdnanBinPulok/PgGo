package modules

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fetchRowResult extracts a single row's data into a map.
func (t *Table) fetchRowResult(rows pgx.Rows, fields []pgconn.FieldDescription) (map[string]interface{}, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to read returned values: %w", err)
	}

	if fields == nil {
		fields = rows.FieldDescriptions()
	}

	result := make(map[string]interface{})
	for i, fd := range fields {
		result[string(fd.Name)] = values[i]
	}
	return result, nil
}

// fetchRowsResult extracts multiple rows' data into a slice of maps.
func (t *Table) fetchRowsResult(rows pgx.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	for rows.Next() {
		fields := rows.FieldDescriptions()
		row, err := t.fetchRowResult(rows, fields)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	return results, nil
}

// QuoteIdentifier safely quotes a SQL identifier (table name, column name).
func QuoteIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// buildWhereClause constructs the WHERE clause and corresponding arguments.
//
// It automatically quotes identifiers in map keys to prevent SQL injection.
// Raw string arguments are assumed to be safe SQL fragments (e.g., "id = $1").
//
// Example input:
//
//	whereArgs: []interface{}{
//	    "id = $1",
//	    map[string]interface{}{"name": "John", "email": "john@example.com"}
//	}
//
// Example output:
//
//	whereClause: " WHERE id = $1 AND \"name\" = $2 AND \"email\" = $3"
//	args: []interface{}{"John", "john@example.com"}
//	argIndex: updated index after processing
func buildWhereClause(whereArgs []interface{}, argIndex *int) (string, []interface{}) {
	conditions := []string{}
	args := []interface{}{}

	for _, arg := range whereArgs {
		switch v := arg.(type) {
		case map[string]interface{}:
			for key, val := range v {
				quotedKey := QuoteIdentifier(key)
				if cond, ok := val.(Condition); ok {
					sql, condArgs := cond.ToSQL(quotedKey, argIndex)
					conditions = append(conditions, sql)
					args = append(args, condArgs...)
				} else {
					conditions = append(conditions, fmt.Sprintf("%s = $%d", quotedKey, *argIndex))
					args = append(args, val)
					*argIndex++
				}
			}

		case string:
			conditions = append(conditions, v)

		default:
			args = append(args, v)
		}
	}

	if len(conditions) == 0 {
		return "", args
	}

	return " WHERE " + strings.Join(conditions, " AND "), args
}
