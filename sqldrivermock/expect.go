package sqldrivermock

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

type expectation interface {
	fulfill(expectation) error
	fulfilled() bool
	fmt.Stringer
}

// ExpectationMismatchError is provided when a recorded event doesn't match expected type
type ExpectationMismatchError struct {
	expected expectation
	actual   expectation
}

func (err ExpectationMismatchError) Error() string {
	return fmt.Sprintf("sqldrivermock: expected %T; got: %T", err.expected, err.actual)
}

// Expected is the expected event
func (err ExpectationMismatchError) Expected() interface{} {
	return err.expected
}

// Actual is the actual event
func (err ExpectationMismatchError) Actual() interface{} {
	return err.actual
}

// Expect is a series of expectations of Driver
type Expect struct {
	expectations []expectation
	next         int
}

func (e *Expect) open(conn *ExpectedConn) (*ExpectedConn, error) {
	if len(e.expectations) <= e.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Open()")
	}

	ex := e.expectations[e.next]
	if err := ex.fulfill(conn); err != nil {
		return nil, err
	}
	e.next++
	return ex.(*ExpectedConn), nil
}

// Open expects a call to driver.Open()
func (e *Expect) Open() *ExpectedConn {
	ex := &ExpectedConn{}
	e.expectations = append(e.expectations, ex)
	return ex
}

// Confirm verifies that all expectations have been met
func (e *Expect) Confirm() error {
	for _, ex := range e.expectations {
		if !ex.fulfilled() {
			return fmt.Errorf("sqldrivermock: unfulfilled expectation: %s", ex)
		}
	}
	return nil
}

func (e *Expect) String() string {
	exStr := make([]string, len(e.expectations))
	for i, ex := range e.expectations {
		exStr[i] = ex.String()
	}
	return fmt.Sprintf("Expect(\n%s\n)", indent(strings.Join(exStr, "\n")))
}

// ExpectedConn is the set of expecatations for a driver.Conn
type ExpectedConn struct {
	dsn string
	err error

	fulfilledBy  *ExpectedConn
	expectations []expectation
	next         int
}

func (ec *ExpectedConn) fulfill(ae expectation) error {
	if ec.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedConn already fulfilled")
	}

	if ac, isa := ae.(*ExpectedConn); isa {
		if ac.dsn != ec.dsn {
			return fmt.Errorf("sqldrivermock: Open() DSN mismatch: expected %#v; got %#v", ec.dsn, ac.dsn)
		}
		ec.fulfilledBy = ac
		return nil
	}
	return ExpectationMismatchError{expected: ec, actual: ae}
}

func (ec *ExpectedConn) fulfilled() bool {
	if ec.fulfilledBy == nil {
		return false
	}
	for _, ex := range ec.expectations {
		if !ex.fulfilled() {
			return false
		}
	}
	return true
}

func (ec *ExpectedConn) String() string {
	exStr := make([]string, len(ec.expectations))
	for i, ex := range ec.expectations {
		exStr[i] = ex.String()
	}
	return fmt.Sprintf("Conn{ DSN: %s, Err: %v } %s (\n%s\n)", ec.dsn, ec.err, fulfilledString(ec.fulfilledBy != nil), indent(strings.Join(exStr, "\n")))
}

// WithDSN sets the expected DSN for the connection
func (ec *ExpectedConn) WithDSN(dsn string) *ExpectedConn {
	ec.dsn = dsn
	return ec
}

// WillError specifies an error that will be returned by driver.Open
func (ec *ExpectedConn) WillError(err error) {
	ec.err = err
}

func (ec *ExpectedConn) begin(tx *ExpectedTx) (*ExpectedTx, error) {
	if len(ec.expectations) <= ec.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Begin() [expectation %d/%d for % #v]", ec.next+1, len(ec.expectations), ec)
	}

	ex := ec.expectations[ec.next]
	if err := ex.fulfill(tx); err != nil {
		return nil, err
	}
	ec.next++
	return ex.(*ExpectedTx), nil
}

// Begin expects a call to driver.Conn.Begin
func (ec *ExpectedConn) Begin() *ExpectedTx {
	tx := &ExpectedTx{}
	ec.expectations = append(ec.expectations, tx)
	return tx
}

func (ec *ExpectedConn) prepare(stmt *ExpectedStmt) (*ExpectedStmt, error) {
	if len(ec.expectations) <= ec.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Prepare() [expectation %d/%d for % #v]", ec.next+1, len(ec.expectations), ec)
	}

	ex := ec.expectations[ec.next]
	if err := ex.fulfill(stmt); err != nil {
		return nil, err
	}
	ec.next++
	return ex.(*ExpectedStmt), nil
}

// Prepare expects a call to drvier.Conn.Prepare
func (ec *ExpectedConn) Prepare() *ExpectedStmt {
	stmt := &ExpectedStmt{}
	ec.expectations = append(ec.expectations, stmt)
	return stmt
}

// ExpectedStmt is the set of expectations for a driver.Stmt
type ExpectedStmt struct {
	queryStr string
	err      error

	fulfilledBy  *ExpectedStmt
	expectations []expectation
	next         int
}

func (es *ExpectedStmt) fulfill(ae expectation) error {
	if es.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedStmt already fulfilled")
	}

	if as, isa := ae.(*ExpectedStmt); isa {
		if as.queryStr != es.queryStr {
			return fmt.Errorf("sqldrivermock: Prepare() query mismatch: expected %#v; got %#v", es.queryStr, as.queryStr)
		}
		es.fulfilledBy = as
		return nil
	}
	return ExpectationMismatchError{expected: es, actual: ae}
}

func (es *ExpectedStmt) fulfilled() bool {
	if es.fulfilledBy == nil {
		return false
	}
	for _, ex := range es.expectations {
		if !ex.fulfilled() {
			return false
		}
	}
	return true
}

func (es *ExpectedStmt) String() string {
	exStr := make([]string, len(es.expectations))
	for i, ex := range es.expectations {
		exStr[i] = ex.String()
	}
	return fmt.Sprintf("Stmt{ Query: %s, Err: %v } %s (\n%s\n)", es.queryStr, es.err, fulfilledString(es.fulfilledBy != nil), indent(strings.Join(exStr, "\n")))
}

// WithQuery sets the expected query string
func (es *ExpectedStmt) WithQuery(qs string) *ExpectedStmt {
	es.queryStr = qs
	return es
}

// WillError specifies an error that will be returned by Prepare
func (es *ExpectedStmt) WillError(err error) {
	es.err = err
}

func (es *ExpectedStmt) query(q *ExpectedQuery) (*ExpectedQuery, error) {
	if len(es.expectations) <= es.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Query() [expectation %d/%d for % #v]", es.next+1, len(es.expectations), es)
	}

	ex := es.expectations[es.next]
	if err := ex.fulfill(q); err != nil {
		return nil, err
	}
	es.next++
	return ex.(*ExpectedQuery), nil
}

// Query expects a call to driver.Stmt.Query
func (es *ExpectedStmt) Query() *ExpectedQuery {
	ex := &ExpectedQuery{}
	es.expectations = append(es.expectations, ex)
	return ex
}

func (es *ExpectedStmt) exec(e *ExpectedExec) (*ExpectedExec, error) {
	if len(es.expectations) <= es.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Exec() [expectation %d/%d for % #v]", es.next+1, len(es.expectations), es)
	}

	ex := es.expectations[es.next]
	if err := ex.fulfill(e); err != nil {
		return nil, err
	}
	es.next++
	return ex.(*ExpectedExec), nil
}

// Exec expects a call to driver.Stmt.Exec
func (es *ExpectedStmt) Exec() *ExpectedExec {
	ex := &ExpectedExec{}
	es.expectations = append(es.expectations, ex)
	return ex
}

// ExpectedQuery is the set of expectations for a call to driver.Stmt.Query
type ExpectedQuery struct {
	args []driver.Value
	err  error

	fulfilledBy *ExpectedQuery
}

func (eq *ExpectedQuery) fulfill(ae expectation) error {
	if eq.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedQuery already fulfilled")
	}

	if aq, isa := ae.(*ExpectedQuery); isa {
		eq.fulfilledBy = aq
		return nil
	}
	return ExpectationMismatchError{expected: eq, actual: ae}
}

func (eq *ExpectedQuery) fulfilled() bool {
	return eq.fulfilledBy != nil
}

func (eq *ExpectedQuery) String() string {
	return fmt.Sprintf("Query{ Args: %v Err: %v } %s", eq.args, eq.err, fulfilledString(eq.fulfilledBy != nil))
}

// WithArgs sets the expected set of arguments for the query
func (eq *ExpectedQuery) WithArgs(args ...driver.Value) *ExpectedQuery {
	eq.args = args
	return eq
}

// WillError specifies an error that will be returned by Query
func (eq *ExpectedQuery) WillError(err error) {
	eq.err = err
}

// ExpectedExec is the set of expectations for a call to driver.Stmt.Query
type ExpectedExec struct {
	args []driver.Value
	err  error

	fulfilledBy *ExpectedExec
}

func (ee *ExpectedExec) fulfill(ae expectation) error {
	if ee.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedExec already fulfilled")
	}

	if aexec, isa := ae.(*ExpectedExec); isa {
		ee.fulfilledBy = aexec
		return nil
	}
	return ExpectationMismatchError{expected: ee, actual: ae}
}

func (ee *ExpectedExec) fulfilled() bool {
	return ee.fulfilledBy != nil
}

func (ee *ExpectedExec) String() string {
	return fmt.Sprintf("Exec{ Args: %v Err: %v } %s", ee.args, ee.err, fulfilledString(ee.fulfilledBy != nil))
}

// WithArgs sets the expected set of arguments for the execution
func (ee *ExpectedExec) WithArgs(args ...driver.Value) *ExpectedExec {
	ee.args = args
	return ee
}

// WillError specifies an error that will be returned by Exec
func (ee *ExpectedExec) WillError(err error) {
	ee.err = err
}

// ExpectedTx is the set of expectations for a driver.Tx
type ExpectedTx struct {
	opts driver.TxOptions
	err  error

	fulfilledBy  *ExpectedTx
	expectations []expectation
	next         int
}

func (et *ExpectedTx) fulfill(ae expectation) error {
	if et.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedTx already fulfilled")
	}

	if at, isa := ae.(*ExpectedTx); isa {
		if et.opts.Isolation != at.opts.Isolation {
			return fmt.Errorf("sqldrivermock: Begin() Isolation mismatch: expected %#v; got %#v", et.opts.Isolation, at.opts.Isolation)
		}
		if et.opts.ReadOnly != at.opts.ReadOnly {
			return fmt.Errorf("sqldrivermock: Begin() ReadOnly: expected %#v; got %#v", et.opts.ReadOnly, at.opts.ReadOnly)
		}

		et.fulfilledBy = at
		return nil
	}
	return ExpectationMismatchError{expected: et, actual: ae}
}

func (et *ExpectedTx) fulfilled() bool {
	if et.fulfilledBy == nil {
		return false
	}
	for _, ex := range et.expectations {
		if !ex.fulfilled() {
			return false
		}
	}
	return true
}

func (et *ExpectedTx) String() string {
	exStr := make([]string, len(et.expectations))
	for i, ex := range et.expectations {
		exStr[i] = ex.String()
	}
	return fmt.Sprintf("Tx{ Options: %+v Err: %v } %s (\n%s\n)", et.opts, et.err, fulfilledString(et.fulfilledBy != nil), indent(strings.Join(exStr, "\n")))
}

// WithOptions sets the expected TxOptions
func (et *ExpectedTx) WithOptions(opts driver.TxOptions) *ExpectedTx {
	et.opts = opts
	return et
}

// WillError provides an error that will be returned by the call to Begin
func (et *ExpectedTx) WillError(err error) *ExpectedTx {
	et.err = err
	return et
}

func (et *ExpectedTx) rollback(rb *ExpectedRollback) (*ExpectedRollback, error) {
	if len(et.expectations) <= et.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Rollback() [expectation %d/%d for % #v]", et.next+1, len(et.expectations), et)
	}

	ex := et.expectations[et.next]
	if err := ex.fulfill(rb); err != nil {
		return nil, err
	}
	et.next++
	return ex.(*ExpectedRollback), nil
}

// Rollback expects a call to driver.Tx.Rollback
func (et *ExpectedTx) Rollback() *ExpectedRollback {
	rb := &ExpectedRollback{}
	et.expectations = append(et.expectations, rb)
	return rb
}

func (et *ExpectedTx) commit(c *ExpectedCommit) (*ExpectedCommit, error) {
	if len(et.expectations) <= et.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Commit() [expectation %d/%d for % #v]", et.next+1, len(et.expectations), et)
	}

	ex := et.expectations[et.next]
	if err := ex.fulfill(c); err != nil {
		return nil, err
	}
	et.next++
	return ex.(*ExpectedCommit), nil
}

// Commit expects a call to driver.Tx.Commit
func (et *ExpectedTx) Commit() *ExpectedCommit {
	c := &ExpectedCommit{}
	et.expectations = append(et.expectations, c)
	return c
}

func (et *ExpectedTx) prepare(stmt *ExpectedStmt) (*ExpectedStmt, error) {
	if len(et.expectations) <= et.next {
		return nil, fmt.Errorf("sqldrivermock: unexpected call to Prepare() [expectation %d/%d for % #v]", et.next+1, len(et.expectations), et)
	}

	ex := et.expectations[et.next]
	if err := ex.fulfill(stmt); err != nil {
		return nil, err
	}
	et.next++
	return ex.(*ExpectedStmt), nil
}

// Prepare expects a call to drvier.Conn.Prepare
func (et *ExpectedTx) Prepare() *ExpectedStmt {
	stmt := &ExpectedStmt{}
	et.expectations = append(et.expectations, stmt)
	return stmt
}

// ExpectedRollback is the set of expectations for a call to driver.Tx.Rollback
type ExpectedRollback struct {
	err error

	fulfilledBy *ExpectedRollback
}

func (er *ExpectedRollback) fulfill(ae expectation) error {
	if er.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedRollback already fulfilled")
	}

	if ar, isa := ae.(*ExpectedRollback); isa {
		er.fulfilledBy = ar
		return nil
	}
	return ExpectationMismatchError{expected: er, actual: ae}
}

func (er *ExpectedRollback) fulfilled() bool {
	return er.fulfilledBy != nil
}

func (er *ExpectedRollback) String() string {
	return fmt.Sprintf("Rollback{ Err: %v } %s", er.err, fulfilledString(er.fulfilledBy != nil))
}

// WillError provides an error that will be returned by the call to Rollback
func (er *ExpectedRollback) WillError(err error) {
	er.err = err
}

// ExpectedCommit is the set of expectations for a call to driver.Tx.Commit
type ExpectedCommit struct {
	err error

	fulfilledBy *ExpectedCommit
}

func (ec *ExpectedCommit) fulfill(ae expectation) error {
	if ec.fulfilledBy != nil {
		return fmt.Errorf("sqldrivermock: ExpectedCommit already fulfilled")
	}

	if ac, isa := ae.(*ExpectedCommit); isa {
		ec.fulfilledBy = ac
		return nil
	}
	return ExpectationMismatchError{expected: ec, actual: ae}
}

func (ec *ExpectedCommit) fulfilled() bool {
	return ec.fulfilledBy != nil
}

func (ec *ExpectedCommit) String() string {
	return fmt.Sprintf("Commit{ Err: %v } %s", ec.err, fulfilledString(ec.fulfilledBy != nil))
}

// WillError provides an error that will be returned by the call to Commit
func (ec *ExpectedCommit) WillError(err error) {
	ec.err = err
}

var indentRegexp *regexp.Regexp

func indent(str string) string {
	return indentRegexp.ReplaceAllString(str, "\t")
}

func fulfilledString(fulfilled bool) string {
	if fulfilled {
		return "✓"
	}
	return "∅"
}

func init() {
	indentRegexp = regexp.MustCompile(`(?m)^`)
}
