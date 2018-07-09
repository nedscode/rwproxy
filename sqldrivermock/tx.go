package sqldrivermock

type tx struct {
	conn *conn
}

func (t *tx) Commit() error {
	return nil
}

func (t *tx) Rollback() error {
	return nil
}
