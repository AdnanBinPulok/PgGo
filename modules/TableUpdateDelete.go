package modules

import (
	"context"
	"fmt"
	"strings"
)

// Update updates rows in the table based on the provided conditions.
//
// It automatically filters out any keys in the data map that do not correspond to defined columns in the table.
// Column names are safely quoted to prevent identifier injection.
// Values are passed as parameters to prevent SQL injection.
//
// Parameters:
//   - data: A map where keys are column names to update and values are the new values.
//   - whereArgs: Conditions to identify which rows to update. Can be a map or raw SQL string with args.
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing the updated rows.
//   - error: An error if the update operation fails or no valid columns are provided.
//
// Example:
//
//	// Update email for user with ID 5
//	updates := map[string]interface{}{"email": "new.email@example.com"}
//	updatedRows, err := UsersTable.Update(updates, "id = $1", 5)
//	if err != nil {
//	    log.Println("Error updating user:", err)
//	}
func (t *Table) Update(data map[string]interface{}, whereArgs ...interface{}) ([]map[string]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data to update")
	}

	// Filter columns to match defined schema (ignore unknown columns)
	validColumns := make(map[string]bool)
	for _, col := range t.Columns {
		validColumns[col.Name] = true
	}

	// 1. Process SET clause
	setParts := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))
	argIndex := 1

	for col, val := range data {
		if validColumns[col] {
			setParts = append(setParts, fmt.Sprintf("%s = $%d", QuoteIdentifier(col), argIndex))
			args = append(args, val)
			argIndex++
		}
	}

	if len(setParts) == 0 {
		return nil, fmt.Errorf("no valid columns provided for update")
	}

	setClause := strings.Join(setParts, ", ")

	// 2. Process WHERE clause
	whereClause, whereArgsList := buildWhereClause(whereArgs, &argIndex)
	args = append(args, whereArgsList...)

	// 3. Process RETURNING clause
	returningClause := " RETURNING *"

	// 4. Build SQL
	updateSQL := fmt.Sprintf("UPDATE %s SET %s%s%s", t.Name, setClause, whereClause, returningClause)

	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	// Execute Query
	rows, err := conn.Query(context.Background(), updateSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute update with returning: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch returned rows: %w", err)
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

	t.invalidateCache()
	return results, nil
}

// Delete deletes rows from the table based on the provided conditions.
//
// It uses parameterized queries for values and quotes identifiers in the WHERE clause (if map syntax is used) to prevent SQL injection.
//
// Parameters:
//   - whereArgs: Conditions to identify which rows to delete. Can be a map or raw SQL string with args.
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing the deleted rows.
//   - error: An error if the delete operation fails.
//
// Example:
//
//	// Delete user with ID 5
//	deletedRows, err := UsersTable.Delete("id = $1", 5)
//	if err != nil {
//	    log.Println("Error deleting user:", err)
//	}
func (t *Table) Delete(whereArgs ...interface{}) ([]map[string]interface{}, error) {
	// 1. Process WHERE clause
	argIndex := 1
	whereClause, whereArgsList := buildWhereClause(whereArgs, &argIndex)
	// 2. Process RETURNING clause
	returningClause := " RETURNING *"

	// 3. Build SQL
	deleteSQL := fmt.Sprintf("DELETE FROM %s%s%s", t.Name, whereClause, returningClause)

	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	// Execute Query
	rows, err := conn.Query(context.Background(), deleteSQL, whereArgsList...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute delete with returning: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch returned rows: %w", err)
	}

	if t.Cached {
		go func(rows []map[string]interface{}) {
			for _, row := range rows {
				if key, err := t.getCacheKey(row); err == nil {
					_ = t.deleteCache(key)
				}
			}
		}(results)
	}

	t.invalidateCache()
	return results, nil
}
