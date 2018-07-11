package rwproxy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

// ConnCloseError is provided when conn.Close() fails, encapsulating errors from one or both proxied connections
type ConnCloseError struct {
	errors []error
}

func (e ConnCloseError) Error() string {
	es := make([]string, len(e.errors))
	for i, err := range e.errors {
		es[i] = err.Error()
	}

	return fmt.Sprintf("rwproxy: failed to close %d proxied connections: %s", len(e.errors), strings.Join(es, ", "))
}

// ErrConnBeginTxUnsupported is provided when conn.BeginTx() is used but not supported by the underlying driver
var ErrConnBeginTxUnsupported = errors.New("rwproxy: driver doesn't support BeginTx")

// conn is a virtual conneciton to a read/write cluster of connections
type conn struct {
	driver *Driver

	writerDSN  string
	readerDSNs []string

	writerConn *proxiedConn
	readerConn *proxiedConn

	tx *tx
}

func (c *conn) writer(ctx context.Context) (*proxiedConn, error) {
	if c.tx != nil {
		return c.tx.driverConn, nil
	}

	var err error
	if c.writerConn == nil {
		c.driver.debugf("opening writer connection to: %s", c.writerDSN)
		pc, err := c.driver.proxiedDriver.Open(c.writerDSN)
		if err != nil {
			return nil, err
		}
		c.writerConn = &proxiedConn{Conn: pc, role: "writer"}
		return c.writerConn, nil
	}
	return c.writerConn, err
}

func (c *conn) reader(ctx context.Context) (*proxiedConn, error) {
	if c.tx != nil {
		return c.tx.driverConn, nil
	}

	var err error
	if c.readerConn == nil {
		// if there's no readers, signal the caller to use a writer instead
		if len(c.readerDSNs) == 0 {
			c.driver.debugf("no readers specified; substituting with writer")
			c.readerConn, err = c.writer(ctx)
			return c.readerConn, err
		}

		// pick a reader
		c.driver.debugf("selecting reader connection from: [ %s ]", strings.Join(c.readerDSNs, "; "))
		pc, err := c.driver.selector(ctx, c.driver.proxiedDriver, c.readerDSNs)
		if err != nil {
			// fall back to signalling the caller to use a writer instead
			c.driver.debugf("no readers available; substituting with writer: %s", err)
			c.readerConn, err = c.writer(ctx)
			return c.readerConn, err
		}
		c.readerConn = &proxiedConn{Conn: pc, role: "reader"}
	}
	return c.readerConn, err
}

// Prepare returns a lazily prepared statement, not yet bound to an underlying connection
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

// Close closes the underlying reader and writer connections
func (c *conn) Close() error {
	errs := []error{}
	if c.writerConn != nil {
		c.driver.debugf("closing writer")
		if err := c.writerConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.readerConn != nil && c.readerConn != c.writerConn {
		c.driver.debugf("closing reader")
		if err := c.readerConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return ConnCloseError{errors: errs}
	}
	return nil
}

// Begin starts and returns a new transaction
func (c *conn) Begin() (driver.Tx, error) {
	if c.tx != nil {
		// already in a transaction
		c.driver.debugf("begin called while already in a transaction")
		return nil, driver.ErrBadConn
	}

	w, err := c.writer(context.Background())
	if err != nil {
		return nil, err
	}

	wtx, err := w.Begin()
	if err != nil {
		return nil, err
	}
	c.tx = &tx{conn: c, driverConn: w, proxiedTx: wtx, closeCh: c.waitCloseTx()}
	return c.tx, nil
}

// BeginTx starts and returns a new transaction
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx != nil {
		// already in a transaction
		c.driver.debugf("begin called while already in a transaction")
		return nil, driver.ErrBadConn
	}

	// read only transactions can be sent to a reader
	if opts.ReadOnly {
		c.driver.debugf("begin readonly transaction; using reader")
		r, err := c.reader(ctx)
		if err == nil {
			if err := c.beginTx(ctx, r, opts); err == nil {
				// transacting on the reader
				return c.tx, nil
			}
		}
		// if any part of the reader transaction setup fails, fall back to the writer
	}

	// by default, force transactions to the writer
	w, err := c.writer(ctx)
	if err != nil {
		return nil, err
	}
	if err = c.beginTx(ctx, w, opts); err == nil {
		// transacting on the writer
		return c.tx, nil
	}
	if err == ErrConnBeginTxUnsupported && opts.Isolation == driver.IsolationLevel(sql.LevelDefault) {
		// if options aren't used, try falling back to plain .Begin()
		return c.Begin()
	}
	return nil, err
}

func (c *conn) beginTx(ctx context.Context, pc *proxiedConn, opts driver.TxOptions) error {
	if b, ok := pc.Conn.(driver.ConnBeginTx); ok {
		dtx, err := b.BeginTx(ctx, opts)
		if err != nil {
			return err
		}
		// no errors, use the reader transaction
		c.tx = &tx{conn: c, driverConn: pc, proxiedTx: dtx, closeCh: c.waitCloseTx()}
		return nil
	}
	return ErrConnBeginTxUnsupported
}

func (c *conn) waitCloseTx() chan<- struct{} {
	close := make(chan struct{}, 1)
	go func() {
		<-close
		c.tx = nil
	}()
	return close
}

// PrepareContext returns a lazily prepared statement, not yet bound to an underlying connection
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// Check the context now (as the statement will be prepared lazily) and return if it's expired
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return &stmt{conn: c, query: query}, nil
}

// Exec attempts to fast-path conn.Exec() against the writer
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	w, err := c.writer(context.Background())
	if err != nil {
		return nil, err
	}
	if e, ok := w.Conn.(driver.Execer); ok {
		return e.Exec(query, args)
	}
	return nil, driver.ErrSkip
}

// ExecContext attempts to fast-path conn.ExecContext() against the writer
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Exec always goes to the writer
	w, err := c.writer(ctx)
	if err != nil {
		return nil, err
	}
	if e, ok := w.Conn.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// Ping forces writer and reader connections to be established and verified
func (c *conn) Ping(ctx context.Context) error {
	// Ping all subconnections (so they can be reconnected if necessary)
	w, err := c.writer(ctx)
	if err != nil {
		return err
	}
	if err := ping(ctx, w); err != nil {
		return err
	}

	r, err := c.reader(ctx)
	if err != nil {
		return err
	}
	if c.readerConn != c.writerConn {
		// only ping the reader if it's a different connection to the writer
		if err := ping(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// Query attempts to fast-path conn.Query() against the reader
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	// Query always goes to the reader
	w, err := c.reader(context.Background())
	if err != nil {
		return nil, err
	}
	if e, ok := w.Conn.(driver.Queryer); ok {
		return e.Query(query, args)
	}
	return nil, driver.ErrSkip
}

// QueryContext attempts to fast-path conn.QueryContext() against the reader
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Query always goes to the reader
	w, err := c.reader(ctx)
	if err != nil {
		return nil, err
	}
	if e, ok := w.Conn.(driver.QueryerContext); ok {
		return e.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func ping(ctx context.Context, conn driver.Conn) error {
	if p, ok := conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}
