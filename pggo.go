package pggo

import (
	"fmt"
	"pggo/modules"
)

// DatabaseConnection represents a connection pool to the PostgreSQL database.
type DatabaseConnection = modules.DatabaseConnection

// Table represents a database table and provides methods for CRUD operations.
type Table = modules.Table

// Column represents a column definition within a Table.
type Column = modules.Column

// ColumnDef represents the data type and constraints of a column.
type ColumnDef = modules.ColumnDef

// Row represents a single row of result data.
type Row = modules.Row

// NewDatabaseConnection creates and initializes a new connection pool to the database.
// It establishes the connection immediately and panics if the connection fails.
//
// Parameters:
//   - dbURL: The PostgreSQL connection string (e.g., "postgres://user:pass@host:port/db").
//   - maxConnections: The maximum number of connections in the pool.
//   - reconnect: Whether to automatically attempt reconnection (handled by pgx pool).
func NewDatabaseConnection(dbURL string, maxConnections int, reconnect bool) *DatabaseConnection {
	conn := &DatabaseConnection{
		DB_URL:          dbURL,
		MAX_CONNECTIONS: maxConnections,
		RECONNECT:       reconnect,
	}
	// Initialize the pool immediately
	_, err := conn.ConnectDb()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize database connection: %v", err))
	}
	return conn
}

// DataType provides a fluent API for defining column types (e.g., DataType.Text(), DataType.Integer()).
var DataType = modules.DataType{}

// In creates a condition checking if a value is within a set of values.
var In = modules.In

// Between creates a condition checking if a value is within a range (inclusive).
var Between = modules.Between

// IsNull creates a condition checking if a value is NULL.
var IsNull = modules.IsNull

// IsNotNull creates a condition checking if a value is NOT NULL.
var IsNotNull = modules.IsNotNull

// Like creates a condition for pattern matching (e.g., LIKE 'abc%').
var Like = modules.Like

// Gt creates a condition checking if a value is greater than the target.
var Gt = modules.Gt

// Lt creates a condition checking if a value is less than the target.
var Lt = modules.Lt

// Gte creates a condition checking if a value is greater than or equal to the target.
var Gte = modules.Gte

// Lte creates a condition checking if a value is less than or equal to the target.
var Lte = modules.Lte

// Neq creates a condition checking if a value is not equal to the target.
var Neq = modules.Neq
