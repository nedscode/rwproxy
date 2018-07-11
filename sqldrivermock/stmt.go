package sqldrivermock

import (
	"database/sql/driver"
)

type stmt struct {
	conn   *conn
	name   string
	expect *ExpectedStmt
}

func (s *stmt) Close() error {
	s.conn.driver.logf("closing %s", s.name)
	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.driver.logf("execing %s", s.name)

	ex, err := s.expect.exec(&ExpectedExec{args: args})
	if err != nil {
		return nil, err
	}
	if ex.err != nil {
		return nil, ex.err
	}
	return &result{}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.driver.logf("querying %s", s.name)

	ex, err := s.expect.query(&ExpectedQuery{args: args})
	if err != nil {
		return nil, err
	}
	if ex.err != nil {
		return nil, ex.err
	}
	return &rows{}, nil
}

func (s *stmt) NumInput() int {
	return -1
}
