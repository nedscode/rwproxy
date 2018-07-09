/*
Package rwproxy provides an implementation of "database/sql/driver".Driver, switching between a "writer" connection and series of "reader" connections of an underlying delegate driver.

Goal

To provide reader-writer query distribution without userland changes in database usage.

Basics

rwproxy achieves its goal by being an implementation of database/sql/driver.Driver. It doesn't provide any direct database driver facilities, and instead wraps around a
delegate driver which provides the concrete implementation:

	sql.Register("mysqlrw", rwproxy.New(mysql.MySQLDriver{})) // makes available a `mysqlrw` driver that reader-writer proxies MySQL connections
	db, _ := sql.Open("my-writer;my-reader-1;my-reader-2") // `db` can be used to query the reader or writer connections

DSNs

The cluster is specified as a "compound" DSN to sql.Open(). The compound DSN is a semicolon-separated (;) list of DSNs for the delegate driver.
The first DSN is used as the "writer", and any subsequent DSNs as a series of "readers" across which queries will be load balanced.
If no readers are specified, all queries will be sent to the writer, which should behave identically to not using rwproxy at all.

Routing

rwproxy selects the most appropriate connection as follows:

	if inTransaction {
		if TxOptions{ReadOnly: true} {
			reader
		} else {
			writer
		}
	} else {
		Exec -> writer
		Query -> reader
	}

The rwproxy *sql.Conn lazily connects to the writer and a single reader as necessary, and will retain these until the it is closed by the connection pool.

Connection Pooling

Package "database/sql" provides a builtin connection pool when sql.Open() is used. Because the pooling happens at a level above (and therefore out of control of) the rwproxy driver,
it is the rwproxy connections (not the delegated connections) that are pooled. This means that, at worst, rwproxy will hold open both a writer and reader connection for each item
in the connection pool.
*/
package rwproxy
