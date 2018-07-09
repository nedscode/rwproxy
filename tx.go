package rwproxy

import (
	"database/sql/driver"
)

type tx struct {
	conn       *conn
	driverConn *proxiedConn
	proxiedTx  driver.Tx
	closeCh    chan<- struct{}
}

func (t *tx) Commit() error {
	t.close()
	return t.proxiedTx.Commit()
}

func (t *tx) Rollback() error {
	t.close()
	return t.proxiedTx.Rollback()
}

func (t *tx) close() {
	select {
	case t.closeCh <- struct{}{}:
	default:
	}
}
