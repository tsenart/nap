package nap

import (
	"database/sql"
	"strings"
	"testing"
	"testing/quick"

	_ "github.com/mattn/go-sqlite3"
)

func TestOpen(t *testing.T) {
	// https://www.sqlite.org/inmemorydb.html
	db, err := Open("sqlite3", ":memory:;:memory:;:memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Error(err)
	}

	if want, got := 3, len(db.pdbs); want != got {
		t.Errorf("Unexpected number of physical dbs. Got: %d, Want: %d", got, want)
	}
}

func TestClose(t *testing.T) {
	db, err := Open("sqlite3", ":memory:;:memory:;:memory:")
	if err != nil {
		t.Fatal(err)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	if err = db.Ping(); err.Error() != "sql: database is closed" {
		t.Errorf("Physical dbs were not closed correctly. Got: %s", err)
	}
}

func TestSlave(t *testing.T) {
	db := &DB{}
	last := -1

	err := quick.Check(func(n int) bool {
		index := db.slave(n)
		if n <= 1 {
			return index == 0
		}

		result := index > 0 && index < n && index != last
		last = index

		return result
	}, nil)

	if err != nil {
		t.Error(err)
	}
}

func TestQueryRow(t *testing.T) {
	// https://www.sqlite.org/inmemorydb.html
	db, err := Open("sqlite3", ":memory:;:memory:;:memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Error(err)
	}

	master := false

	db.SetQueryRowDB(func(query string, args ...interface{}) *sql.DB {
		if len(query) > 12 && strings.ToLower(query)[0:12] == "insert into " {
			master = true
			return db.Master()
		}
		master = false
		return db.Slave()
	})

	res := db.QueryRow("insert into t1(c1,c2) values(1,1);", nil)
	if res == nil {
		t.Errorf("func QueryRow has no results")
	}
	if !master {
		t.Errorf("query row expected to use master database")
	}

	res = db.QueryRow("select * from t1", nil)
	if res == nil {
		t.Errorf("func QueryRow has no results")
	}
	if master {
		t.Errorf("query row expected to use slave database")
	}
}
