package modules

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Table represents a database table structure and configuration.
// It serves as the main entry point for performing CRUD operations on a specific table.
type Table struct {
	// Name is the name of the table in the database.
	Name string
	// Connection is the database connection pool interface.
	Connection DatabaseConnection
	// Columns is a list of column definitions for the table.
	Columns []Column
	// Cached enables in-memory caching for this table.
	Cached bool
	// CacheTTL defines the time-to-live for cached items.
	CacheTTL time.Duration
	// CacheKey is the column name used as the key for caching (usually the primary key).
	CacheKey string
	// CacheMax is the maximum number of items to store in the cache.
	CacheMax int
	// CacheData holds the actual in-memory cache instance.
	CacheData *MemoryCache
	// DebugMode enables verbose logging of SQL queries and operations.
	DebugMode bool
}

// Column represents a single column definition in a database table.
type Column struct {
	// Name is the column name in the database.
	Name string
	// DataType defines the column's type and constraints (e.g., INTEGER, TEXT, UNIQUE).
	DataType ColumnDef
}

// Row is an alias for pgx.Row, representing a single row of results.
type Row = pgx.Row

// isDefinedColumnUnique checks if a column has a UNIQUE constraint defined in the table schema.
func (t *Table) isDefinedColumnUnique(column Column) bool {
	for _, col := range t.Columns {
		if col.Name == column.Name {
			if strings.Contains(col.DataType.String(), "UNIQUE") {
				return true
			}
		}
	}
	return false
}

// getDefinedColumnNames returns a slice of all column names defined in the Table struct.
func (t *Table) getDefinedColumnNames() []string {
	var cols []string
	for _, col := range t.Columns {
		cols = append(cols, col.Name)
	}
	return cols
}

// CreateTable creates the table in the database if it does not exist.
// It constructs a CREATE TABLE SQL statement based on the Table struct's Name and Columns.
// It automatically quotes table and column names to prevent SQL injection.
// After creating the table, it synchronizes the columns by adding missing ones and removing obsolete ones.
//
// Example:
//
//	err := usersTable.CreateTable()
//	if err != nil {
//	    log.Fatalf("Failed to create table: %v", err)
//	}
func (t *Table) CreateTable() error {
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return err
	}
	// Release connection back to pool when function exits
	defer conn.Release()

	var columnDefs []string
	for _, col := range t.Columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", QuoteIdentifier(col.Name), col.DataType.String()))
	}
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", QuoteIdentifier(t.Name), strings.Join(columnDefs, ", "))
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	t.createCurrentColumn()
	t.deleteNonExistingColumnsFromDB()

	return nil
}

// GetColumnsFromDB retrieves the list of column names for the table from the database's information_schema.
//
// Returns:
//   - []string: A slice of column names found in the database.
//   - error: An error if the query fails.
//
// Example:
//
//	cols, err := table.GetColumnsFromDB()
//	if err != nil {
//	    log.Println("Error fetching columns:", err)
//	}
func (t *Table) GetColumnsFromDB() ([]string, error) {
	// will get from database
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return nil, err
	}
	// Release connection back to pool when function exits
	defer conn.Release()

	const QueryString = "SELECT column_name  FROM information_schema.columns WHERE table_name = $1"
	rows, err := conn.Query(context.Background(), QueryString, t.Name)
	if err != nil {
		return nil, err
	}

	fmt.Printf("DEBUG: Rows: %v\n", rows)

	defer rows.Close() // Also close the rows when done

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// columnExists checks if a specific column definition exists in the list of database columns.
func (t *Table) columnExists(column Column, db_columns []string) bool {
	for _, col := range db_columns {
		if col == column.Name {
			return true
		}
	}
	return false
}

// createCurrentColumn ensures that all columns defined in the Table struct exist in the database.
// It adds any missing columns.
func (t *Table) createCurrentColumn() (bool, error) {
	db_columns, err := t.GetColumnsFromDB()
	if err != nil {
		return false, err
	}
	for _, col := range t.Columns {
		if !t.columnExists(col, db_columns) {
			t.addColumn(Column{Name: col.Name, DataType: col.DataType}) // Default to TEXT type
		}
	}
	return true, nil
}

// columnNotExists checks if a column name from the database does NOT exist in the Table struct's definition.
func (t *Table) columnNotExists(column string, db_columns []Column) bool {
	for _, col := range db_columns {
		if col.Name == column {
			return false
		}
	}
	return true
}

// removeColumn drops a column from the table in the database.
// It automatically quotes the table and column names to prevent SQL injection.
//
// Parameters:
//   - column: The name of the column to remove.
//
// Returns:
//   - bool: true if the column was successfully removed, false otherwise.
//
// Example:
//
//	success := table.removeColumn("obsolete_column")
//	if !success {
//	    log.Println("Failed to remove column")
//	}
func (t *Table) removeColumn(column string) bool {
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return false
	}
	defer conn.Release()

	fmt.Printf("Removing column <%s> from table <%s>\n", column, t.Name)
	removeColumnSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", QuoteIdentifier(t.Name), QuoteIdentifier(column))
	_, err = conn.Exec(context.Background(), removeColumnSQL)
	if err != nil {
		fmt.Printf("Error removing column: %v\n", err)
		return false
	}
	fmt.Println("DEBUG: SQL executed successfully For Removing column:", column)

	return true
}

// deleteNonExistingColumnsFromDB removes columns from the database that are not present in the Table struct.
func (t *Table) deleteNonExistingColumnsFromDB() (bool, error) {
	db_columns, err := t.GetColumnsFromDB()
	if err != nil {
		return false, err
	}
	for _, col := range db_columns {
		if t.columnNotExists(col, t.Columns) {
			t.removeColumn(col)
		}
	}
	return true, nil
}

// addColumn adds a new column to the table in the database.
// It automatically quotes the table and column names to prevent SQL injection.
//
// Parameters:
//   - column: The Column struct defining the name and data type of the new column.
//
// Returns:
//   - bool: true if the column was successfully added, false otherwise.
//
// Example:
//
//	newCol := Column{Name: "age", DataType: Integer}
//	success := table.addColumn(newCol)
//	if !success {
//	    log.Println("Failed to add column")
//	}
func (t *Table) addColumn(column Column) bool {
	fmt.Printf("Adding column <%s> of type <%s> to table <%s>\n", column.Name, column.DataType.String(), t.Name)

	conn, err := t.Connection.GetConnection()
	if err != nil {
		return false
	}
	defer conn.Release()

	var columnType string

	if column.DataType == (ColumnDef{}) {
		columnType = "TEXT"
	} else {
		columnType = column.DataType.String()
	}

	fmt.Printf("DEBUG: Prepared to execute SQL to add column %s of type %s\n", column.Name, columnType)
	addColumnSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", QuoteIdentifier(t.Name), QuoteIdentifier(column.Name), columnType)
	_, err = conn.Exec(context.Background(), addColumnSQL)
	if err != nil {
		fmt.Printf("Error adding column: %v\n", err)
		return false
	}
	fmt.Println("DEBUG: SQL executed successfully For Adding column:", column.Name)

	t.Columns = append(t.Columns, column)
	return true
}

// DropTable drops the table from the database.
// It automatically quotes the table name to prevent SQL injection.
//
// Returns:
//   - error: An error if the drop operation fails.
//
// Example:
//
//	err := table.DropTable()
//	if err != nil {
//	    log.Printf("Failed to drop table: %v", err)
//	}
func (t *Table) DropTable() error {
	conn, err := t.Connection.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Release()

	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", QuoteIdentifier(t.Name))
	_, err = conn.Exec(context.Background(), dropTableSQL)
	if err != nil {
		fmt.Printf("Error dropping table: %v\n", err)
		return err
	}
	fmt.Println("DEBUG: SQL executed successfully For Dropping table:", t.Name)

	return nil
}

// GetTableName returns the name of the table.
//
// Example:
//
//	name := table.GetTableName()
//	fmt.Println("Table name:", name)
func (t *Table) GetTableName() string {
	return t.Name
}
