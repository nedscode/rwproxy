package sqldrivermock

import (
	"database/sql/driver"
)

type stmt struct {
	conn *conn
	name string
}

func (s *stmt) Close() error {
	s.conn.driver.logf("closing %s", s.name)
	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.driver.logf("execing %s", s.name)
	return &result{}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.driver.logf("querying %s", s.name)
	return &rows{}, nil
}

func (s *stmt) NumInput() int {
	return -1
}
