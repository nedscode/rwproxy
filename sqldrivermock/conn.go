package sqldrivermock

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type conn struct {
	driver *Driver
	name   string
	stmts  int
	expect *ExpectedConn
	tx     *tx
}

func newConn(d *Driver, name string, ex *ExpectedConn) driver.Conn {
	return &conn{driver: d, name: name, expect: ex}
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.begin(context.Background(), driver.TxOptions{})
}

func (c *conn) begin(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	ex, err := c.expect.begin(&ExpectedTx{opts: opts})
	if err != nil {
		return nil, err
	}
	if ex.err != nil {
		return nil, ex.err
	}
	c.tx = &tx{expect: ex}
	return c.tx, nil

}

func (c *conn) Close() error {
	c.logf("closing: %s", c.name)
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	c.stmts++

	name := fmt.Sprintf("%s.Prepared[%d]", c.name, c.stmts)
	c.logf("preparing %s: %#v", name, query)

	type preparer interface {
		prepare(*ExpectedStmt) (*ExpectedStmt, error)
	}
	var p preparer = c.expect
	if c.tx != nil {
		p = c.tx.expect
	}
	ex, err := p.prepare(&ExpectedStmt{queryStr: query})
	if err != nil {
		return nil, err
	}
	if ex.err != nil {
		return nil, ex.err
	}
	return &stmt{conn: c, name: name, expect: ex}, nil
}

func (c *conn) logf(format string, args ...interface{}) {
	c.driver.logf(format, args...)
}

type connBeginTx struct {
	*conn
}

func (c *connBeginTx) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.begin(ctx, opts)
}

func newConnBeginTx(d *Driver, name string, ex *ExpectedConn) driver.Conn {
	return &connBeginTx{conn: newConn(d, name, ex).(*conn)}
}
