// +build mysql

package rwproxy_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/nedscode/rwproxy"
)

// mysqlDSN exposed as an override point for build-time connection details
var mysqlDSN = ""

func TestDriverMySQL(t *testing.T) {
	if mysqlDSN == "" {
		c := mysql.NewConfig()
		c.Net = "tcp"
		c.Addr = "localhost"
		c.User = "root"
		c.Passwd = ""
		mysqlDSN = c.FormatDSN()
	}

	d := rwproxy.New(&mysql.MySQLDriver{}, rwproxy.WithLog(func(v string) { t.Log(v) }))
	dname := t.Name()
	sql.Register(dname, d)

	db, err := sql.Open(dname, mysqlDSN)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Set connection limits for determinism
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	// two sequential connections
	t.Run("sequential", func(t *testing.T) { testMySQLConnection(t, ctx, db) })
	t.Run("sequential", func(t *testing.T) { testMySQLConnection(t, ctx, db) })

	// two parallel connections over 4 runs
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() { t.Run("parallel", func(t *testing.T) { testMySQLConnection(t, ctx, db) }); wg.Done() }()
	go func() { t.Run("parallel", func(t *testing.T) { testMySQLConnection(t, ctx, db) }); wg.Done() }()
	go func() { t.Run("parallel", func(t *testing.T) { testMySQLConnection(t, ctx, db) }); wg.Done() }()
	go func() { t.Run("parallel", func(t *testing.T) { testMySQLConnection(t, ctx, db) }); wg.Done() }()
	wg.Wait()
}

func testMySQLConnection(t *testing.T, ctx context.Context, db *sql.DB) {
	// Force a connection
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer conn.Close()

	if err := conn.PingContext(ctx); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testMySQLStatements(t, ctx, conn)
	testMySQLPreparedStatements(t, ctx, conn)

	// Transaction
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testMySQLStatements(t, ctx, tx)
	testMySQLPreparedStatements(t, ctx, tx)
	tx.Rollback()

	// Transaction (readonly)
	tx, err = conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testMySQLStatements(t, ctx, tx)
	testMySQLPreparedStatements(t, ctx, tx)
	tx.Rollback()
}

func testMySQLStatements(t *testing.T, ctx context.Context, q queryer) {
	// Query
	t.Log("single query")
	rows, err := q.QueryContext(ctx, "SELECT 666")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	var selected int
	rows.Next()
	rows.Scan(&selected)
	rows.Close()

	if selected != 666 {
		t.Errorf("expected selected to be 666, got %d", selected)
	}

	// Exec
	t.Log("single exec")
	_, err = q.ExecContext(ctx, "SELECT 2")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func testMySQLPreparedStatements(t *testing.T, ctx context.Context, p preparer) {
	testMySQLPreparedStatementsQueryFirst(t, ctx, p)
	testMySQLPreparedStatementsExecFirst(t, ctx, p)
}

func testMySQLPreparedStatementsExecFirst(t *testing.T, ctx context.Context, p preparer) {
	// Prepare
	t.Log("reused statement: prepare")
	stmt, err := p.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer stmt.Close()

	// Prepared Query
	t.Log("reused statement: query")
	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rows.Close()

	// Prepared Exec
	t.Log("reused statement: exec")
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func testMySQLPreparedStatementsQueryFirst(t *testing.T, ctx context.Context, p preparer) {
	// Prepare
	t.Log("reused statement: prepare")
	stmt, err := p.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer stmt.Close()

	// Prepared Exec
	t.Log("reused statement: exec")
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Prepared Query
	t.Log("reused statement: query")
	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rows.Close()
}
