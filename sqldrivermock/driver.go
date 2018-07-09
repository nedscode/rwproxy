// Package sqldrivermock provides a basic null implementation of "database/sql/driver".Driver
package sqldrivermock

import (
	"database/sql/driver"
	"fmt"
)

// Driver is a mock implementation of database/sql/driver.Driver
type Driver struct {
	Logf  func(string, ...interface{})
	conns int
}

// Open opens a new mock connection
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.conns++
	name = fmt.Sprintf("%s[%d]", name, d.conns)
	d.logf("opening: %s", name)
	return &conn{driver: d, name: name}, nil
}

func (d *Driver) logf(format string, args ...interface{}) {
	if d.Logf != nil {
		d.Logf(format, args...)
	}
}
