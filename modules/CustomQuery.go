package modules

import (
	"context"
	"fmt"
	"log"
)

// Queue executes a custom raw SQL query against the database.
//
// Safety Note: This method executes raw SQL. Always use parameterized queries ($1, $2, etc.)
// for any user-provided input to prevent SQL injection. Do not concatenate user input directly into the query string.
//
// Parameters:
//   - query: The SQL query string to execute (e.g., "SELECT * FROM users WHERE id = $1").
//   - params: Variadic arguments representing the parameters for the query placeholders.
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing the result rows.
//   - error: An error if the query execution fails.
//
// Example:
//
//	query := "SELECT * FROM users WHERE age > $1"
//	results, err := UsersTable.Queue(query, 20)
//	if err != nil {
//	    log.Println("Error executing custom query:", err)
//	}
func (t *Table) Queue(query string, params ...interface{}) ([]map[string]interface{}, error) {
	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	if t.DebugMode {
		log.Println("DEBUG: Executing Custom Query:", query, "Params:", params)
	}

	// Execute Query
	rows, err := conn.Query(context.Background(), query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute custom query: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	// Fetch results
	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows: %w", err)
	}

	return results, nil
}
