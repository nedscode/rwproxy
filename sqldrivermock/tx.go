package sqldrivermock

type tx struct {
	conn   *conn
	expect *ExpectedTx
}

func (t *tx) Commit() error {
	ex, err := t.expect.commit(&ExpectedCommit{})
	if err != nil {
		return err
	}
	return ex.err
}

func (t *tx) Rollback() error {
	ex, err := t.expect.rollback(&ExpectedRollback{})
	if err != nil {
		return err
	}
	return ex.err
}
