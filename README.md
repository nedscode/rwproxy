# rwproxy: reader-writer query-distributing proxy driver

## Goal

To provide reader-writer query distribution without userland changes in database usage.

## Basics

`rwproxy` achieves its goal by being an implementation of `database/sql/driver.Driver`. It doesn't provide any direct database driver facilities, and instead wraps around a delegate driver which provides the concrete implementation:

## Getting Started

Install `rwproxy` with `go get`:

```sh
go get github.com/nedscode/rwproxy
```

Package `rwproxy` provides an implementation of ``"database/sql/driver".Driver``, switching between a *"writer"* connection and series of *"reader"* connections of an underlying delegate driver.

```go
sql.Register("mysqlrw", rwproxy.New(mysql.MySQLDriver{})) // makes available a `mysqlrw` driver that reader-writer proxies MySQL connections
db, _ := sql.Open("my-writer;my-reader-1;my-reader-2") // `db` can be used to query the reader or writer connections
```

## DSNs

The cluster is specified as a "compound" DSN to `sql.Open().` The compound DSN is a semicolon-separated (`;`) list of DSNs for the delegate driver. The first DSN is used as the *"writer"*, and any subsequent DSNs as a series of *"readers"* across which queries will be load balanced. If no readers are specified, all queries will be sent to the writer, which should behave identically to not using `rwproxy` at all.

## Routing

`rwproxy` selects the most appropriate connection as follows:

```
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
```

The `rwproxy` `*sql.Conn` lazily connects to the writer and a single reader as necessary, and will retain these until it is closed by the connection pool.

## Connection Pooling

Package `"database/sql"` provides a builtin connection pool when `sql.Open()` is used. Because the pooling happens at a level above (and therefore out of control of) the `rwproxy` driver, it is the `rwproxy` connections (not the delegated connections) that are pooled. This means that, at worst, `rwproxy` will hold open both a writer and reader connection for each item in the connection pool.

## FAQ

### Is there any way to force `Query` to run on the writer, or `Exec` to run on a reader?

Use database transactions to force the connection to the reader or writer:

```go
wtx, _ := sql.Begin()
wtx.QueryRow() // will run against the writer, because the transaction is assumed to require writes

rtx, _ := sql.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
rtx.Exec() // will run against a reader, because the transaction is marked as read only
```

### I need to perform a write, then read that (or a derived) value back from the database. How can I ensure consistency?

Use a database transaction across the write and read operations:

```go
 // Query() is run on the same transaction (and therefore connection) as the Exec()
tx, := sql.Begin()
tx.Exec("UPDATE …")
tx.Query("SELECT …")
tx.Commit()

// also possible, but less desirable
tx := sql.Begin()
tx.Exec("UPDATE …")
tx.Commit()
// (in a different scope - forces Query to writer)
tx := sql.Begin()
tx.Query("SELECT …")
tx.Commit()
```

## Acknowledgements

* [github.com/tsenart/nap](https://github.com/tsenart/nap) provides similar functionality, though as a wrapper around `database/sql`, rather than as a driver.
