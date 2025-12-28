package modules

import (
	"context"
	"fmt"
	"log"
)

// FetchOne fetches a single row from the table based on the provided arguments.
//
// It accepts variable arguments to specify conditions for filtering.
//   - Strings are treated as raw SQL fragments (e.g., "id = $1").
//   - A map[string]interface{} is treated as WHERE conditions (ANDed together).
//
// If no columns are specified, it selects all columns (*).
//
// It uses parameterized queries for values and quotes identifiers in the WHERE clause (if map syntax is used) to prevent SQL injection.
//
// Example usage:
// option 1:
//
//	userData, err := UsersTable.FetchOne(map[string]interface{}{"email": "admin@gmail.com"})
//
// option 2:
//
//	userData, err := UsersTable.FetchOne(map[string]interface{}{"id": 5})
//
// Returns:
//   - map[string]interface{}: A map representing the fetched row.
//   - error: An error if the operation fails or no rows are found.
func (t *Table) FetchOne(whereArgs ...interface{}) (map[string]interface{}, error) {
	// Try to fetch from cache first
	if t.Cached {
		if key, err := t.getCacheKey(whereArgs...); err == nil {
			var cachedResult map[string]interface{}
			if found, _ := t.getCacheValue(key, &cachedResult); found {
				if t.DebugMode {
					log.Println("✅ Returning Cached Hit")
				}
				return cachedResult, nil
			}
		}
	}

	argIndex := 1

	where_clause, params := buildWhereClause(whereArgs, &argIndex)
	selectSQL := fmt.Sprintf("SELECT * FROM %s%s LIMIT 1", t.Name, where_clause)
	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer conn.Release() // Release connection back to pool when done

	if t.DebugMode {
		log.Println("DEBUG: Executing FetchOne with SQL:", selectSQL, "Params:", params)
	}

	rows, err := conn.Query(context.Background(), selectSQL, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute fetch one: %w", err)
	}
	defer rows.Close() // Also close the rows when done

	if !rows.Next() {
		return nil, fmt.Errorf("no rows found")
	}
	result, err := t.fetchRowResult(rows, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch row: %w", err)
	}

	// Save to cache
	if t.Cached {
		if t.DebugMode {
			log.Println("DEBUG: FetchOne - Attempting to set cache")
		}
		if key, err := t.getCacheKey(result); err == nil {
			_ = t.setCache(key, result)
		} else {
			if t.DebugMode {
				log.Println("DEBUG: FetchOne - getCacheKey failed:", err)
			}
		}
	} else {
		if t.DebugMode {
			log.Println("DEBUG: FetchOne - Caching NOT enabled")
		}
	}

	return result, nil
}

// FetchMany fetches multiple rows from the table based on the provided arguments.
// It accepts variable arguments to specify conditions for filtering.
//
// It uses parameterized queries for values and quotes identifiers in the WHERE clause (if map syntax is used) to prevent SQL injection.
//
// Example:
//
//	users, err := UsersTable.FetchMany(map[string]interface{}{"active": true})
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing the fetched rows.
//   - error: An error if the operation fails.
func (t *Table) FetchMany(whereArgs ...interface{}) ([]map[string]interface{}, error) {
	argIndex := 1
	where_clause, params := buildWhereClause(whereArgs, &argIndex)
	selectSQL := fmt.Sprintf("SELECT * FROM %s%s", t.Name, where_clause)
	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	if t.DebugMode {
		log.Println("DEBUG: Executing FetchMany with SQL:", selectSQL, "Params:", params)
	}

	rows, err := conn.Query(context.Background(), selectSQL, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute fetch many: %w", err)
	}

	defer rows.Close() // Also close the rows when done

	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows: %w", err)
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

// GetPage fetches a paginated list of rows.
// page: Page number (starts at 1). Defaults to 1 if <= 0.
// limit: Number of items per page. Defaults to 10 if <= 0.
// orderBy: Column to sort by. Defaults to "id" if empty.
// order: Sort direction ("ASC" or "DESC"). Defaults to "DESC" if empty.
// whereArgs: Conditions for filtering (same as FetchMany).
func (t *Table) GetPage(page, limit int, orderBy, order string, whereArgs ...interface{}) ([]map[string]interface{}, error) {
	if page <= 0 {
		page = 1
	}
	if limit <= 0 {
		limit = 10
	}
	if orderBy == "" {
		orderBy = "id"
	}
	if order == "" {
		order = "DESC"
	}

	offset := (page - 1) * limit
	argIndex := 1
	whereClause, params := buildWhereClause(whereArgs, &argIndex)

	// Add pagination and sorting
	query := fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s %s LIMIT %d OFFSET %d",
		t.Name, whereClause, orderBy, order, limit, offset)

	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	if t.DebugMode {
		log.Println("DEBUG: Executing GetPage with SQL:", query, "Params:", params)
	}

	rows, err := conn.Query(context.Background(), query, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetPage: %w", err)
	}
	defer rows.Close()

	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows: %w", err)
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

// GetPageWithTotal fetches a paginated list of rows and the total count of rows matching the criteria.
// page: Page number (starts at 1). Defaults to 1 if <= 0.
// limit: Number of items per page. Defaults to 10 if <= 0.
// orderBy: Column to sort by. Defaults to "id" if empty.
// order: Sort direction ("ASC" or "DESC"). Defaults to "DESC" if empty.
// whereArgs: Conditions for filtering (same as FetchMany).
// Returns:
// - []map[string]interface{}: The rows for the current page.
// - int64: The total number of rows matching the criteria.
// - error: An error if the operation fails.
func (t *Table) GetPageWithTotal(page, limit int, orderBy, order string, whereArgs ...interface{}) ([]map[string]interface{}, int64, error) {
	if page <= 0 {
		page = 1
	}
	if limit <= 0 {
		limit = 10
	}
	if orderBy == "" {
		orderBy = "id"
	}
	if order == "" {
		order = "DESC"
	}

	offset := (page - 1) * limit
	argIndex := 1
	whereClause, params := buildWhereClause(whereArgs, &argIndex)

	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// 1. Get Total Count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", t.Name, whereClause)
	var totalCount int64
	err = conn.QueryRow(context.Background(), countQuery, params...).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get total count: %w", err)
	}

	// 2. Get Data
	query := fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s %s LIMIT %d OFFSET %d",
		t.Name, whereClause, orderBy, order, limit, offset)

	if t.DebugMode {
		log.Println("DEBUG: Executing GetPageWithTotal with SQL:", query, "Params:", params)
	}

	rows, err := conn.Query(context.Background(), query, params...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute GetPageWithTotal: %w", err)
	}
	defer rows.Close()

	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch rows: %w", err)
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

	return results, totalCount, nil
}

// FetchAll retrieves all rows from the table.
//
// It automatically quotes the table name to ensure safety.
//
// Returns:
//   - []map[string]interface{}: A slice of maps representing all rows in the table.
//   - error: An error if the operation fails.
//
// Example:
//
//	allUsers, err := UsersTable.FetchAll()
//	if err != nil {
//	    log.Println("Error fetching all users:", err)
//	}
func (t *Table) FetchAll() ([]map[string]interface{}, error) {
	// Acquire connection from pool
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Release connection back to pool when done

	selectSQL := fmt.Sprintf("SELECT * FROM %s", t.Name)
	rows, err := conn.Query(context.Background(), selectSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute get all: %w", err)
	}
	defer rows.Close() // Also close the rows when done
	results, err := t.fetchRowsResult(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows: %w", err)
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
