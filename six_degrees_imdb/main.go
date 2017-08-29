package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "user=aulty dbname=aulty sslmode=verify-full")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM title_principals WHERE age = $1", age)

}
