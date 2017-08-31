package main

import (
	"log"

	"github.com/aultimus/six_degrees_imdb"

	_ "github.com/lib/pq"
)

func main() {
	app := sixdegreesimdb.NewApp()
	err := app.Init()
	if err != nil {
		log.Fatal(err)
	}

	err = app.Run()
	if err != nil {
		log.Fatal(err)
	}
}
