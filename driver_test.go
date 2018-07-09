package rwproxy_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/nedscode/rwproxy"
	"github.com/nedscode/rwproxy/sqldrivermock"
)

type queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type preparer interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

func TestDriver(t *testing.T) {
	d := rwproxy.New(&sqldrivermock.Driver{Logf: t.Logf})
	dname := t.Name()
	sql.Register(dname, d)

	db, err := sql.Open(dname, "my-writer")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer db.Close()

	// Set connection limits for determinism
	db.SetMaxOpenConns(1)

	// Force a connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer conn.Close()

	if err := conn.PingContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testStatements(t, conn)
	testPreparedStatements(t, conn)

	// Transaction
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testStatements(t, tx)
	testPreparedStatements(t, tx)
	tx.Rollback()
}

func testStatements(t *testing.T, q queryer) {
	// Query
	t.Log("single query")
	rows, err := q.QueryContext(context.Background(), "SELECT")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rows.Close()

	// Exec
	t.Log("single exec")
	_, err = q.ExecContext(context.Background(), "UPDATE")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func testPreparedStatements(t *testing.T, p preparer) {
	// Prepare
	t.Log("reused statement: prepare")
	stmt, err := p.PrepareContext(context.Background(), "SELECT")
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
