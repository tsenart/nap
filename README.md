# Nap

Nap is a library that abstracts access to master-slave physical SQL servers topologies as a single logical database mimicking the standard `sql.DB` APIs.

## Install
```shell
$ go get github.com/tsenart/nap
```

## Usage
```go
package main

import (
  "log"

  "github.com/tsenart/nap"
  _ "github.com/go-sql-driver/mysql" // Any sql.DB works
)

func main() {
  // The first DSN is assumed to be the master and all
  // other to be slaves
  dsns := "tcp://user:password@master/dbname;"
  dsns += "tcp://user:password@slave01/dbname;"
  dsns += "tcp://user:password@slave02/dbname"

  db, err := nap.Open("mysql", dsns)
  if err != nil {
    log.Fatal(err)
  }
  
  if err := db.Ping(); err != nil {
    log.Fatalf("Some physical database is unreachable: %s", err)
  }

  // Read queries are directed to slaves with Query and QueryRow.
  // Always use Query or QueryRow for SELECTS
  // Load distribution is round-robin only for now.
  var count int
  err = db.QueryRow("SELECT COUNT(*) FROM sometable").Scan(&count)
  if err != nil {
    log.Fatal(err)
  }

  // Write queries are directed to the master with Exec.
  // Always use Exec for INSERTS, UPDATES
  err = db.Exec("UPDATE sometable SET something = 1")
  if err != nil {
    log.Fatal(err)
  }

  // Prepared statements are aggregates. If any of the underlying
  // physical databases fails to prepare the statement, the call will
  // return an error. On success, if Exec is called, then the
  // master is used, if Query or QueryRow are called, then a slave
  // is used.
  stmt, err := db.Prepare("SELECT * FROM sometable WHERE something = ?")
  if err != nil {
    log.Fatal(err)
  }

  // Transactions always use the master
  tx, err := db.Begin()
  if err != nil {
    log.Fatal(err)
  }
  // Do something transactional ...
  if err = tx.Commit(); err != nil {
    log.Fatal(err)
  }

  // If needed, one can access the master or a slave explicitly.
  master, slave := db.Master(), db.Slave()
}
```

## Todo
* Support other slave load balancing algorithms.

## License
See [LICENSE](LICENSE)
