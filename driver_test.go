package rwproxy_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

func newRegisteredMockProxy(t *testing.T, rwproxyOpts []rwproxy.Option, mockOpts []sqldrivermock.Option) (string, *rwproxy.Driver, *sqldrivermock.Driver) {
	mockDrv := sqldrivermock.New(mockOpts...)
	rwproxyDrv := rwproxy.New(mockDrv, rwproxyOpts...)
	name := t.Name()
	sql.Register(name, rwproxyDrv)
	return name, rwproxyDrv, mockDrv
}

func TestDriver(t *testing.T) {
	dname, _, mockDrv := newRegisteredMockProxy(t, nil, []sqldrivermock.Option{sqldrivermock.ConnBeginTx()})
	expect := mockDrv.Expect()
	defer func() {
		if t.Failed() {
			t.Log(expect.String())
		}
	}()

	db, err := sql.Open(dname, "my-writer;my-reader")
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
	// defer conn.Close()

	exConnW := expect.Open().WithDSN("my-writer")
	exConnR := expect.Open().WithDSN("my-reader")
	if err := conn.PingContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	exConnR.Prepare().WithQuery("SELECT").Query()
	exConnW.Prepare().WithQuery("UPDATE").Exec()
	testStatements(t, conn)
	exConnR.Prepare().WithQuery("SELECT").Query()
	exConnW.Prepare().WithQuery("SELECT").Exec()
	testPreparedStatements(t, conn)

	// Transaction
	exTx := exConnW.Begin()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	exTx.Prepare().WithQuery("SELECT").Query()
	exTx.Prepare().WithQuery("UPDATE").Exec()
	testStatements(t, tx)
	exStmt := exTx.Prepare().WithQuery("SELECT")
	exStmt.Query()
	exStmt.Exec()
	testPreparedStatements(t, tx)
	exTx.Rollback()
	tx.Rollback()

	// Confirm all expectations met
	if err := expect.Confirm(); err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
}

func testStatements(t *testing.T, q queryer) {
	// Query
	rows, err := q.QueryContext(context.Background(), "SELECT")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rows.Close()

	// Exec
	_, err = q.ExecContext(context.Background(), "UPDATE")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func testPreparedStatements(t *testing.T, p preparer) {
	// Prepare
	stmt, err := p.PrepareContext(context.Background(), "SELECT")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer stmt.Close()

	// Prepared Query
	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rows.Close()

	// Prepared Exec
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestBeginTxFallback(t *testing.T) {
	dname, _, mockDrv := newRegisteredMockProxy(t, nil, nil)
	expect := mockDrv.Expect()

	// Expectations
	// // write at default level → writer conn
	exConnW := expect.Open().WithDSN("my-writer")
	exConnW.Begin().Rollback()
	// // write at custom level → ErrConnBeginTxUnsupported
	// none
	// // read → writer conn
	expect.Open().WithDSN("my-reader")
	exConnW.Begin().Rollback()

	defer func() {
		if t.Failed() {
			t.Log(expect.String())
		}
	}()

	db, err := sql.Open(dname, "my-writer;my-reader")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer db.Close()

	// Set connection limits for determinism
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer conn.Close()

	// write at default level → writer conn
	wtx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := wtx.Rollback(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// write at custom level → ErrConnBeginTxUnsupported
	_, err = conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != rwproxy.ErrConnBeginTxUnsupported {
		t.Errorf("error mismatch: expected %s, got %s", rwproxy.ErrConnBeginTxUnsupported, err)
		t.Log(expect.String())
		return
	}

	// read → writer conn
	rtx, err := conn.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		t.Log(expect.String())
		return
	}
	if err := rtx.Rollback(); err != nil {
		t.Errorf("unexpected error: %s", err)
		t.Log(expect.String())
		return
	}

	// Confirm all expectations met
	if err := expect.Confirm(); err != nil {
		t.Errorf("unexpected error: %s", err)
		t.Log(expect.String())
		return
	}
}

func TestStmt_reuse(t *testing.T) {
	cases := []struct {
		name        string
		rwproxyOpts []rwproxy.Option
		mockOpts    []sqldrivermock.Option
		expect      func(*sqldrivermock.Expect)
		do          func(t *testing.T, driver string)
	}{
		{
			name:     "implicit read; forced write; forced read",
			mockOpts: []sqldrivermock.Option{sqldrivermock.ConnBeginTx()},
			expect: func(expect *sqldrivermock.Expect) {
				exConnR := expect.Open().WithDSN("reader")
				exStmtR := exConnR.Prepare().WithQuery("SELECT")
				exStmtR.Query()

				exConnW := expect.Open().WithDSN("writer")
				exTxW := exConnW.Begin()
				exTxW.Prepare().WithQuery("SELECT").Query()
				exTxW.Rollback()

				exTxR := exConnR.Begin().WithOptions(driver.TxOptions{ReadOnly: true})
				exStmtR.Query()
				exTxR.Rollback()
			},
			do: func(t *testing.T, driver string) {
				db, err := sql.Open(driver, "writer;reader")
				if err != nil {
					t.Fatalf("expected error: %s", err)
				}
				defer db.Close()

				// Prepare
				stmt, err := db.PrepareContext(context.Background(), "SELECT")
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				defer stmt.Close()

				// Implicitly reader
				ir, err := stmt.Query()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				ir.Close()

				// Force to writer
				wtx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				wr, err := wtx.Stmt(stmt).Query()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				wr.Close()
				wtx.Rollback()

				// Force to reader
				rtx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				rr, err := rtx.Stmt(stmt).Query()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				rr.Close()
				rtx.Rollback()
			},
		},
		{
			name:     "implicit write; forced read; forced write",
			mockOpts: []sqldrivermock.Option{sqldrivermock.ConnBeginTx()},
			expect: func(expect *sqldrivermock.Expect) {
				exConnW := expect.Open().WithDSN("writer")
				exStmtW := exConnW.Prepare().WithQuery("SELECT")
				exStmtW.Exec()

				expect.Open().WithDSN("reader").
					Begin().WithOptions(driver.TxOptions{ReadOnly: true}).
					Prepare().WithQuery("SELECT").
					Exec()

				exTxW := exConnW.Begin()
				exStmtW.Exec()
				exTxW.Rollback()
			},
			do: func(t *testing.T, driver string) {
				db, err := sql.Open(driver, "writer;reader")
				if err != nil {
					t.Fatalf("expected error: %s", err)
				}
				defer db.Close()

				// Prepare
				stmt, err := db.PrepareContext(context.Background(), "SELECT")
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				defer stmt.Close()

				// Implicitly writer
				_, err = stmt.Exec()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				// Force to reader
				rtx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				_, err = rtx.Stmt(stmt).Exec()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				rtx.Rollback()

				// Force to writer
				wtx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}

				_, err = wtx.Stmt(stmt).Exec()
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				wtx.Rollback()
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			drvName, _, mockDrv := newRegisteredMockProxy(t, nil, []sqldrivermock.Option{sqldrivermock.ConnBeginTx()})
			defer func() {
				if t.Failed() {
					t.Log(mockDrv.Expect().String())
				}
			}()

			c.expect(mockDrv.Expect())
			defer func() {
				if t.Failed() {
					t.Log(mockDrv.Expect().String())
				}
			}()

			c.do(t, drvName)

			if err := mockDrv.Expect().Confirm(); err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}
