package rwproxy

import (
	"context"
	"database/sql/driver"
)

// Option is a configuration option for a Driver instance
type Option func(*Driver)

// WithReaderSelector creates an Option for the given ReaderSelector implementation
func WithReaderSelector(rs ReaderSelector) Option {
	return func(d *Driver) {
		d.selector = rs
	}
}

// RoundRobinReaderSelector implements a round robin strategy for selecting a reader by DSN
func RoundRobinReaderSelector() ReaderSelector {
	next := 0
	return func(ctx context.Context, d driver.Driver, dsns []string) (driver.Conn, error) {
		dsn := dsns[next]
		next = (next + 1) % len(dsns)
		return d.Open(dsn)
	}
}

// WithLog creates an Option for the given Log implementation
//
// The log will be called with near-trace-level debugging to inspect proxying behaviour
func WithLog(l Log) Option {
	return func(d *Driver) {
		d.logFunc = l
	}
}
