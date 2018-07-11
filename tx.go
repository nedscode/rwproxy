package rwproxy

import (
	"database/sql/driver"
)

type tx struct {
	conn       *conn
	driverConn *proxiedConn
	proxiedTx  driver.Tx
}

func (t *tx) Commit() error {
	commitErr := t.proxiedTx.Commit()
	closeErr := t.close()

	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (t *tx) Rollback() error {
	rbErr := t.proxiedTx.Rollback()
	closeErr := t.close()

	if rbErr != nil {
		return rbErr
	}
	return closeErr
}

func (t *tx) close() error {
	return t.conn.closeTx(t)
}
