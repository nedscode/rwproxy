// Package sqldrivermock provides a basic null implementation of "database/sql/driver".Driver
package sqldrivermock

import (
	"database/sql/driver"
	"fmt"
)

type connFactory func(d *Driver, name string, ex *ExpectedConn) driver.Conn

// Driver is a mock implementation of database/sql/driver.Driver
type Driver struct {
	Logf        func(string, ...interface{})
	conns       int
	connFactory connFactory
	expect      *Expect
}

// New creates a Driver
func New(opts ...Option) *Driver {
	d := &Driver{connFactory: newConn, expect: &Expect{expectations: []expectation{}}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Open opens a new mock connection
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.conns++
	desc := fmt.Sprintf("%s[%d]", name, d.conns)
	d.logf("opening: %s", desc)

	ex, err := d.expect.open(&ExpectedConn{dsn: name})
	if err != nil {
		return nil, err
	}
	if ex.err != nil {
		return nil, ex.err
	}
	return d.connFactory(d, desc, ex), nil
}

// Expect is the set of expectations for the mock driver
func (d *Driver) Expect() *Expect {
	return d.expect
}

func (d *Driver) logf(format string, args ...interface{}) {
	if d.Logf != nil {
		d.Logf(format, args...)
	}
}

// Option is a Driver option
type Option func(*Driver)

// Logf provides a logger to Driver
func Logf(fn func(string, ...interface{})) Option {
	return func(d *Driver) {
		d.Logf = fn
	}
}

// ConnBeginTx uses a driver.Conn implementation that also supports driver.ConnBeginTx
func ConnBeginTx() Option {
	return func(d *Driver) {
		d.connFactory = newConnBeginTx
	}
}
