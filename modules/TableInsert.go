package modules

import (
	"context"
	"fmt"
	"strings"
)

// Insert inserts a single row into the table.
//
// It automatically filters out any keys in the data map that do not correspond to defined columns in the table.
// Column names are safely quoted to prevent identifier injection.
// Values are passed as parameters to prevent SQL injection.
//
// Parameters:
//   - data: A map where keys are column names and values are the data to insert.
//
// Returns:
//   - map[string]interface{}: The inserted row data, including any auto-generated fields (like ID).
//   - error: An error if the insert operation fails or if no valid columns are provided.
func (t *Table) Insert(data map[string]interface{}) (map[string]interface{}, error) {
	// Build columns and args
	columns := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	// Filter columns to match defined schema (ignore unknown columns)
	validColumns := make(map[string]bool)
	for _, col := range t.Columns {
		validColumns[col.Name] = true
	}

	for col, val := range data {
		if validColumns[col] {
			columns = append(columns, QuoteIdentifier(col))
			args = append(args, val)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no valid columns provided for insert")
	}

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	returningClause := " RETURNING *"

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)%s",
		t.Name,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		returningClause,
	)

	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	// Execute QueryRow
	rows, err := conn.Query(context.Background(), insertSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute insert with returning: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned")
	}

	result, err := t.fetchRowResult(rows, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch returned row: %w", err)
	}

	if t.Cached {
		go func(row map[string]interface{}) {
			if key, err := t.getCacheKey(row); err == nil {
				_ = t.setCache(key, row)
			}
		}(result)
	}

	return result, nil
}

// InsertMany inserts multiple rows into the table in a single query.
//
// It assumes that all maps in the dataList have the same set of keys.
// It filters columns based on the table definition and quotes identifiers for security.
//
// Parameters:
//   - dataList: A slice of maps, where each map represents a row to insert.
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing the inserted rows.
//   - error: An error if the insert operation fails.
func (t *Table) InsertMany(dataList []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(dataList) == 0 {
		return nil, fmt.Errorf("no data provided to insert")
	}

	var results []map[string]interface{}

	// Filter columns to match defined schema
	validColumns := make(map[string]bool)
	for _, col := range t.Columns {
		validColumns[col.Name] = true
	}

	// Determine columns from the first row, filtering invalid ones
	columns := make([]string, 0)
	rawColumns := make([]string, 0) // Keep raw names for looking up values
	for col := range dataList[0] {
		if validColumns[col] {
			columns = append(columns, QuoteIdentifier(col))
			rawColumns = append(rawColumns, col)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no valid columns found in the first row of dataList")
	}

	// Build placeholders and args
	valuePlaceholders := make([]string, 0, len(dataList))
	args := make([]interface{}, 0)
	argIndex := 1

	for _, data := range dataList {
		placeholders := make([]string, len(columns))
		for i, colName := range rawColumns {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, data[colName])
			argIndex++
		}
		valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	returningClause := " RETURNING *"

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s%s",
		t.Name,
		strings.Join(columns, ", "),
		strings.Join(valuePlaceholders, ", "),
		returningClause,
	)
	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	// Execute Query
	rows, err := conn.Query(context.Background(), insertSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute insert many with returning: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	results, err = t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch returned rows: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows returned")
	}

	if t.Cached {
		go func(rows []map[string]interface{}) {
			for _, row := range rows {
				if key, err := t.getCacheKey(row); err == nil {
					_ = t.setCache(key, row)
				}
			}
		}(results)
	}

	return results, nil
}
