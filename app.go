package sixdegreesimdb

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/mux"
)

const (
	// url path strings
	actorPathA = "actor_a"
	actorPathB = "actor_b"
)

type App struct {
	server *http.Server
	db     *sql.DB
}

func NewApp() *App {
	return &App{}
}

func (a *App) Init() error {
	router := mux.NewRouter()
	// TODO: use string constants for these args
	router.HandleFunc(fmt.Sprintf("/path_between/{%s}/{%s}", actorPathA, actorPathB),
		a.PathBetweenHandler).Methods(http.MethodGet)

	server := &http.Server{
		Addr:           ":8080",
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	a.server = server

	// by default go sql client seems to try to connect over tcp prompting a password
	// so we need to use this brittle string
	db, err := sql.Open("postgres", "postgresql:///aulty?host=/var/run/postgresql")
	if err != nil {
		return err
	}
	// confirm connection to db succeeded
	err = db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (a *App) Run() error {
	return a.server.ListenAndServe()
}

// PathBetweenHandler for the given two actors attempts to find the shortest
// path between them in terms of actors they have worked with
func (a *App) PathBetweenHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	spew.Dump(vars)
	actorA := vars[actorPathA]
	actorB := vars[actorPathB]

	log.Printf("Calculating path between %s and %s\n", actorA, actorB)

	// lookup nconst for actors

	// do BFS

	w.Write([]byte("foo"))
}

//rows, err := db.Query("SELECT * FROM title_principals")
//if err != nil {
//	log.Fatal(err)
//}
//spew.Dump(rows)
