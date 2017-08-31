package main

import (
	"database/sql"
	"log"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/lib/pq"
)

func main() {
	// by default go sql client seems to try to connect over tcp prompting a password
	// so we need to use this brittle string
	db, err := sql.Open("postgres", "postgresql:///aulty?host=/var/run/postgresql")
	if err != nil {
		log.Fatal(err)
	}
	// confirm connection to db succeeded
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM title_principals")
	if err != nil {
		log.Fatal(err)
	}
	spew.Dump(rows)

}
