package modules

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseConnection manages the connection pool to the PostgreSQL database.
// It handles connection configuration, initialization, and automatic reconnection monitoring.
type DatabaseConnection struct {
	// DB_URL is the connection string for the database.
	DB_URL string
	// MAX_CONNECTIONS is the maximum number of connections allowed in the pool.
	MAX_CONNECTIONS int
	// RECONNECT enables or disables the background reconnection monitor.
	RECONNECT bool
	// SavedPoolDbConnection holds the active pgx connection pool.
	SavedPoolDbConnection *pgxpool.Pool
	// ReconnectionCheckRunning indicates if the reconnection monitor is currently active.
	ReconnectionCheckRunning bool
}

// ConnectDb initializes the database connection pool using the configured settings.
// It parses the DB_URL, sets the maximum and minimum connections, and establishes the pool.
// Returns the created pgxpool.Pool or an error if connection fails.
func (conf *DatabaseConnection) ConnectDb() (*pgxpool.Pool, error) {
	ctx := context.Background()
	// Use pgxpool instead of pgx.Connect
	poolConfig, err := pgxpool.ParseConfig(conf.DB_URL)
	if err != nil {
		return nil, err
	}

	log.Printf("Connecting to database at %s with max %d connections...\n", conf.DB_URL, conf.MAX_CONNECTIONS)

	poolConfig.MaxConns = int32(conf.MAX_CONNECTIONS)
	poolConfig.MinConns = int32(conf.MAX_CONNECTIONS / 4)

	poolConnection, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	conf.SavedPoolDbConnection = poolConnection
	return poolConnection, nil
}

// reconnectDb attempts to re-establish the database connection.
// It calls ConnectDb internally.
func (conf *DatabaseConnection) reconnectDb() (bool, error) {
	var err error
	success, err := conf.ConnectDb()
	if err != nil {
		return false, err
	}
	return success != nil, nil
}

// isAlive checks if the current database connection is active and responsive.
// It executes a simple ping to verify connectivity.
func (conf *DatabaseConnection) isAlive() bool {
	return conf.SavedPoolDbConnection != nil
}

func (conf *DatabaseConnection) getPool() (*pgxpool.Pool, error) {
	if conf.SavedPoolDbConnection == nil {
		poolConnection, err := conf.ConnectDb()
		if err != nil {
			return nil, fmt.Errorf("failed to connect to database: %v", err)
		}
		return poolConnection, nil
	}
	return conf.SavedPoolDbConnection, nil
}

func (conf *DatabaseConnection) GetConnection() (*pgxpool.Conn, error) {
	pool, err := conf.getPool()
	if err != nil {
		return nil, err
	}
	return pool.Acquire(context.Background())
}

func (conf *DatabaseConnection) showStats() {
	if conf.SavedPoolDbConnection == nil {
		log.Println("ERROR: Connection pool is not initialized.")
		return
	}
	totalConnections := conf.SavedPoolDbConnection.Stat().TotalConns()
	activeConnections := conf.SavedPoolDbConnection.Stat().TotalConns() - conf.SavedPoolDbConnection.Stat().IdleConns()
	idleConnections := conf.SavedPoolDbConnection.Stat().IdleConns()

	log.Printf("DEBUG: Total connections: %d, Active connections: %d, Idle connections: %d\n", totalConnections, activeConnections, idleConnections)
}

func (conf *DatabaseConnection) CheckDbConnection() (bool, error) {
	if !conf.isAlive() {
		return false, fmt.Errorf("connection pool is not alive")
	}
	// Removed debug print for performance

	// Get a connection from the pool to test it
	conn, err := conf.GetConnection()
	if err != nil {
		return false, err
	}
	defer conn.Release()

	err = conn.Ping(context.Background())
	if err != nil {
		conf.reconnectDb()
		return false, err
	}
	return true, nil
}

// StartDbConnectionChecker starts a goroutine that checks the DB connection every 5 seconds.
func (conf *DatabaseConnection) StartDbConnectionChecker() {
	go func() {
		for {
			conf.CheckDbConnection()
			time.Sleep(5 * time.Second)
		}
	}()
	conf.ReconnectionCheckRunning = true
}
