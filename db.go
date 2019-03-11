package nap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"time"
)

// DB is a logical database with multiple underlying physical databases
// forming a single master multiple slaves topology.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	pdbs  []*sql.DB // Physical databases
	count uint64    // Monotonically incrementing counter on each query
}

// Open concurrently opens each underlying physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func Open(driverName, dataSourceNames string) (*DB, error) {
	conns := strings.Split(dataSourceNames, ";")
	db := &DB{pdbs: make([]*sql.DB, len(conns))}

	err := scatter(len(db.pdbs), func(i int) (err error) {
		db.pdbs[i], err = sql.Open(driverName, conns[i])
		return err
	})

	if err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].Close()
	})
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.Master().Driver()
}

// Begin starts a transaction on the master. The isolation level is dependent on the driver.
func (db *DB) Begin() (*sql.Tx, error) {
	return db.Master().Begin()
}

// BeginTx starts a transaction with the provided context on the master.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.Master().BeginTx(ctx, opts)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Master().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Master().ExecContext(ctx, query, args...)
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].Ping()
	})
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	return scatter(len(db.pdbs), func(i int) error {
		return db.pdbs[i].PingContext(ctx)
	})
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *DB) Prepare(query string) (Stmt, error) {
	stmts := make([]*sql.Stmt, len(db.pdbs))

	err := scatter(len(db.pdbs), func(i int) (err error) {
		stmts[i], err = db.pdbs[i].Prepare(query)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &stmt{db: db, stmts: stmts}, nil
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmts := make([]*sql.Stmt, len(db.pdbs))

	err := scatter(len(db.pdbs), func(i int) (err error) {
		stmts[i], err = db.pdbs[i].PrepareContext(ctx, query)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &stmt{db: db, stmts: stmts}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a slave as the physical db.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.Slave().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a slave as the physical db.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.Slave().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a slave as the physical db.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.Slave().QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a slave as the physical db.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.Slave().QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	for i := range db.pdbs {
		db.pdbs[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.pdbs {
		db.pdbs[i].SetConnMaxLifetime(d)
	}
}

// Slave returns one of the physical databases which is a slave
func (db *DB) Slave() *sql.DB {
	return db.pdbs[db.slave(len(db.pdbs))]
}

// Master returns the master physical database
func (db *DB) Master() *sql.DB {
	return db.pdbs[0]
}

func (db *DB) slave(n int) int {
	if n <= 1 {
		return 0
	}
	return int(1 + (atomic.AddUint64(&db.count, 1) % uint64(n-1)))
}
