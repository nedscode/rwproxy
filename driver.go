package rwproxy

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

// IncompleteDSNError indicates that the compound DSN is incomplete, and cannot be used
type IncompleteDSNError struct {
	DSN string
}

func (e IncompleteDSNError) Error() string {
	return fmt.Sprintf("rwproxy: combination DSN is incomplete: %#v", e.DSN)
}

type proxiedConn struct {
	driver.Conn
	role string
}

// ReaderSelector implements a read distribution strategy
type ReaderSelector func(ctx context.Context, d driver.Driver, readerDSNs []string) (driver.Conn, error)

// Log is function that is called with near-trace-level debugging to inspect proxying behaviour
type Log func(string)

// Driver is a "database/sql/driver".Driver implemntation that distributes reads/writes
type Driver struct {
	proxiedDriver driver.Driver
	writerDSN     string
	readerDSNs    []string
	selector      ReaderSelector
	logFunc       Log
}

// New wraps a lower level delegate "database/sql/driver".Driver with an rwproxy driver
func New(delegate driver.Driver, opts ...Option) *Driver {
	d := &Driver{proxiedDriver: delegate}
	for _, o := range opts {
		o(d)
	}

	// defaults
	if d.selector == nil {
		d.selector = RoundRobinReaderSelector()
	}

	return d
}

// Open implements "database/sql/driver".Driver.Open(), taking a compound DSN containing DSNs for writer and reader connections
func (d *Driver) Open(name string) (driver.Conn, error) {
	wdsn, rdsns := ParseCompoundDSN(name)
	if wdsn == "" {
		// no writer provided, can't proceed
		return nil, IncompleteDSNError{DSN: name}
	}
	return &conn{driver: d, writerDSN: wdsn, readerDSNs: rdsns}, nil
}

// Parent returns the wrapped Driver
func (d *Driver) Parent() driver.Driver {
	return d.proxiedDriver
}

func (d *Driver) debugf(format string, args ...interface{}) {
	if d.logFunc != nil {
		d.logFunc("rwproxy: " + fmt.Sprintf(format, args...))
	}
}

// MakeCompoundDSN combines writer and reader DSNs to build a compound DSN
func MakeCompoundDSN(writerDSN string, readerDSNs ...string) string {
	return strings.Join(append([]string{writerDSN}, readerDSNs...), ";")
}

// ParseCompoundDSN breaks up a compound DSN into its component DSNs
func ParseCompoundDSN(dsn string) (string, []string) {
	// lazily break up between semicolons
	split := strings.Split(dsn, ";")
	dsns := []string{}
	for _, dsn := range split {
		if dsn != "" {
			dsns = append(dsns, dsn)
		}
	}
	return dsns[0], dsns[1:]
}
