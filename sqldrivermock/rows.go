package sqldrivermock

import (
	"database/sql/driver"
)

type rows struct {
	stmt *stmt
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Columns() []string {
	return []string{}
}

func (r *rows) Next(values []driver.Value) error {
	return nil
}
