package rwproxy

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

// ErrNamedParametersNotSupported is provided when named parameters are used but unsupported by the underlying driver
var ErrNamedParametersNotSupported = errors.New("rwproxy: driver does not support the use of Named Parameters")

// ProxiedStatementCloseError is provided when an rwproxy Stmt can't close one of its proxied Stmts
type ProxiedStatementCloseError struct {
	Errs []error
}

func (pscerr ProxiedStatementCloseError) Error() string {
	errStrs := make([]string, len(pscerr.Errs))
	for i, err := range pscerr.Errs {
		errStrs[i] = err.Error()
	}

	return fmt.Sprintf("rwproxy: failed to close proxied statments: %s", strings.Join(errStrs, "; "))
}

const stmtNumInputUninitialised = -2

type stmt struct {
	conn  *conn
	query string

	numInput     int
	proxiedStmts map[driver.Conn]driver.Stmt
}

func newStmt(c *conn, query string) *stmt {
	return &stmt{
		conn:         c,
		query:        query,
		proxiedStmts: map[driver.Conn]driver.Stmt{},
		numInput:     stmtNumInputUninitialised,
	}
}

// Close closes the underlying statement
func (s *stmt) Close() error {
	if len(s.proxiedStmts) == 0 {
		s.conn.driver.debugf("attempted to close unbound statement: %s", s.query)
		return nil
	}

	var errs []error
	for _, proxiedStmt := range s.proxiedStmts {
		s.conn.driver.debugf("closing statement: %s", s.query)
		if err := proxiedStmt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ProxiedStatementCloseError{Errs: errs}
	}
	return nil
}

// NumInput returns the number of placeholder parameters if the statement has already been prepared
func (s *stmt) NumInput() int {
	if len(s.proxiedStmts) > 0 {
		if s.numInput == stmtNumInputUninitialised {
			// Pick a statement: they will all have the same number of inputs
			for _, stmt := range s.proxiedStmts {
				s.numInput = stmt.NumInput()
				return s.numInput
			}
		}
		return s.numInput
	}
	return -1
}

// Exec executes a query that doesn't return rows against the writer
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	c, err := s.conn.writer(context.Background())
	if err != nil {
		return nil, err
	}

	ps, err := s.prepared(context.Background(), c)
	if err != nil {
		return nil, err
	}
	return ps.Exec(args)
}

// Query executes a query that may return rows against the reader
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	c, err := s.conn.reader(context.Background())
	if err != nil {
		return nil, err
	}

	ps, err := s.prepared(context.Background(), c)
	if err != nil {
		return nil, err
	}
	return ps.Query(args)
}

// ExecContext executes a query that doesn't return rows against the writer
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	c, err := s.conn.writer(ctx)
	if err != nil {
		return nil, err
	}

	ps, err := s.prepared(ctx, c)
	if err != nil {
		return nil, err
	}

	if e, ok := ps.(driver.StmtExecContext); ok {
		return e.ExecContext(ctx, args)
	}
	argValues, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return ps.Exec(argValues)
}

// QueryContext executes a query that may return rows against the reader
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	c, err := s.conn.reader(ctx)
	if err != nil {
		return nil, err
	}

	ps, err := s.prepared(ctx, c)
	if err != nil {
		return nil, err
	}

	if e, ok := ps.(driver.StmtQueryContext); ok {
		return e.QueryContext(ctx, args)
	}
	argValues, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return ps.Query(argValues)
}

func (s *stmt) prepared(ctx context.Context, pc *proxiedConn) (driver.Stmt, error) {
	if _, exists := s.proxiedStmts[pc]; !exists {
		s.conn.driver.debugf("preparing statement for %s: %s", pc.role, s.query)
		ps, err := s.prepare(ctx, pc)
		if err != nil {
			return nil, err
		}
		s.proxiedStmts[pc] = ps
	}
	return s.proxiedStmts[pc], nil
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
