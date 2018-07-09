package sqldrivermock

import (
	"database/sql/driver"
	"fmt"
)

type conn struct {
	driver *Driver
	name   string
	stmts  int
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{}, nil
}

func (c *conn) Close() error {
	c.logf("closing: %s", c.name)
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	c.stmts++
	name := fmt.Sprintf("%s.Prepared[%d]", c.name, c.stmts)
	c.logf("preparing %s: %#v", name, query)
	return &stmt{conn: c, name: name}, nil
}

func (c *conn) logf(format string, args ...interface{}) {
	c.driver.logf(format, args...)
}
