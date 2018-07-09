package rwproxy

import (
	"context"
	"database/sql/driver"
	"errors"
)

// ErrNamedParametersNotSupported is provided when named parameters are used but unsupported by the underlying driver
var ErrNamedParametersNotSupported = errors.New("rwproxy: driver does not support the use of Named Parameters")

type stmt struct {
	conn        *conn
	query       string
	proxiedStmt driver.Stmt
}

// Close closes the underlying statement
func (s *stmt) Close() error {
	if s.proxiedStmt != nil {
		s.conn.driver.debugf("closing statement: %s", s.query)
		return s.proxiedStmt.Close()
	}
	s.conn.driver.debugf("attempted to close unbound statement: %s", s.query)
	return nil
}

// NumInput returns the number of placeholder parameters if the statement has already been prepared
func (s *stmt) NumInput() int {
	if s.proxiedStmt != nil {
		return s.proxiedStmt.NumInput()
	}
	return -1
}

// Exec executes a query that doesn't return rows against the writer
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.proxiedStmt == nil {
		c, err := s.conn.writer(context.Background())
		if err != nil {
			return nil, err
		}
		s.conn.driver.debugf("binding exec statement to %s: %s", c.role, s.query)
		s.proxiedStmt, err = s.prepare(context.Background(), c)
		if err != nil {
			return nil, err
		}
	}
	return s.proxiedStmt.Exec(args)
}

// Query executes a query that may return rows against the reader
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.proxiedStmt == nil {
		c, err := s.conn.reader(context.Background())
		if err != nil {
			return nil, err
		}
		s.conn.driver.debugf("binding query statement to %s: %s", c.role, s.query)
		s.proxiedStmt, err = s.prepare(context.Background(), c)
		if err != nil {
			return nil, err
		}
	}
	return s.proxiedStmt.Query(args)
}

// ExecContext executes a query that doesn't return rows against the writer
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.proxiedStmt == nil {
		c, err := s.conn.writer(ctx)
		if err != nil {
			return nil, err
		}
		s.conn.driver.debugf("binding exec statement to %s: %s", c.role, s.query)
		s.proxiedStmt, err = s.prepare(ctx, c)
		if err != nil {
			return nil, err
		}
	}

	if e, ok := s.proxiedStmt.(driver.StmtExecContext); ok {
		return e.ExecContext(ctx, args)
	}
	argValues, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.proxiedStmt.Exec(argValues)
}

// QueryContext executes a query that may return rows against the reader
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.proxiedStmt == nil {
		c, err := s.conn.reader(ctx)
		if err != nil {
			return nil, err
		}
		s.conn.driver.debugf("binding query statement to %s: %s", c.role, s.query)
		s.proxiedStmt, err = s.prepare(ctx, c)
		if err != nil {
			return nil, err
		}
	}

	if e, ok := s.proxiedStmt.(driver.StmtQueryContext); ok {
		return e.QueryContext(ctx, args)
	}
	argValues, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.proxiedStmt.Query(argValues)
}

func (s *stmt) prepare(ctx context.Context, conn driver.Conn) (driver.Stmt, error) {
	if p, ok := conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, s.query)
	}
	return conn.Prepare(s.query)
}

func namedValuesToValues(named []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(named))
	for i, n := range named {
		if n.Name != "" {
			return nil, ErrNamedParametersNotSupported
		}
		values[i] = n.Value
	}
	return values, nil
}
