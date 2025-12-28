# PgGo

PgGo is a high-performance, secure, and easy-to-use PostgreSQL ORM/Query Builder for Go, featuring built-in caching and a fluent API. It is designed to be production-ready, offering robust protection against SQL injection and efficient data handling.

## Features

- **Easy Connection**: Simple setup for PostgreSQL connections with connection pooling.
- **Fluent API**: Intuitive methods for `Insert`, `Select`, `Update`, and `Delete`.
- **Built-in Caching**: Automatic caching support (in-memory) to reduce database load and improve latency.
- **Security First**: 
    - **Parameterized Queries**: All values are passed as parameters to prevent SQL injection.
    - **Identifier Quoting**: All table and column names are safely quoted to prevent identifier injection.
    - **Column Filtering**: Automatically filters out invalid columns to prevent errors and potential attacks.
- **Advanced Filtering**: Support for `Gt`, `Lt`, `In`, `Like`, `Between`, `IsNull`, etc.
- **Production Ready**: Thread-safe logging and robust error handling.

## Installation

```bash
go get github.com/yourusername/pggo
```

*(Note: Replace `github.com/yourusername/pggo` with the actual module path if published)*

## Usage

### 1. Connect to Database

```go
package main

import (
	"log"
	"pggo"
)

func main() {
	dbURL := "postgres://user:password@localhost:5432/dbname"
	// Connect with max 20 connections and auto-reconnect enabled
	connection := pggo.NewDatabaseConnection(dbURL, 20, true)
	log.Println("Connected to database")
}
```

### 2. Define a Table

```go
UsersTable := pggo.Table{
    Name:       "users",
    Connection: *connection,
    Columns: []pggo.Column{
        {Name: "id", DataType: *pggo.DataType.Serial().PrimaryKey()},
        {Name: "name", DataType: *pggo.DataType.Text().NotNull()},
        {Name: "email", DataType: *pggo.DataType.Text().Unique().NotNull()},
        {Name: "age", DataType: *pggo.DataType.Integer()},
    },
    DebugMode: true, // Enable for verbose logging
}

// Enable Caching (Optional)
UsersTable.CacheKey = "id"
UsersTable.EnableCache(5 * time.Second)

// Create Table if not exists
err := UsersTable.CreateTable()
```

### 3. Insert Data

```go
user := map[string]interface{}{
    "name":  "Alice",
    "email": "alice@example.com",
    "age":   25,
}
insertedUser, err := UsersTable.Insert(user)
if err != nil {
    log.Fatal(err)
}
log.Printf("Inserted User ID: %v", insertedUser["id"])
```

### 4. Fetch Data

**Fetch One by ID:**
```go
user, err := UsersTable.FetchOne(map[string]interface{}{"id": 1})
```

**Fetch Many with Conditions:**
```go
// Fetch users older than 20
users, err := UsersTable.FetchMany(map[string]interface{}{
    "age": pggo.Gt(20),
})
```

**Advanced Filtering:**
```go
users, err := UsersTable.FetchMany(map[string]interface{}{
    "name": pggo.Like("Ali%"),
    "age":  pggo.Between(20, 30),
})
```

### 5. Update Data

```go
// Update Alice's age to 26
updates := map[string]interface{}{"age": 26}
updatedRows, err := UsersTable.Update(updates, map[string]interface{}{"id": 1})
```

### 6. Delete Data

```go
// Delete user with ID 1
deletedRows, err := UsersTable.Delete(map[string]interface{}{"id": 1})
```

## Security

PgGo takes security seriously:

- **Value Injection**: All user-provided values are passed to the database using parameterized queries (`$1`, `$2`, etc.), making value-based SQL injection mathematically impossible.
- **Identifier Injection**: All column names and table identifiers are wrapped in double quotes (e.g., `"column_name"`). This ensures that even if a malicious key is passed (though keys should generally be trusted), it is treated as a literal identifier and not executable SQL.
- **Input Sanitization**: `Insert` and `Update` methods automatically filter out any keys that do not match the defined table schema.

## License

MIT License
