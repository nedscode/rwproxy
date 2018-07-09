package sqldrivermock

type result struct {
	stmt *stmt
}

func (r *result) LastInsertId() (int64, error) {
	return -1, nil
}

func (r *result) RowsAffected() (int64, error) {
	return -1, nil
}
