package sixdegreesimdb

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

const (
	actorPathA  = "Actor A"
	actorPathB  = "Actor B"
	nconstPathA = "nconst A"
	nconstPathB = "nconst B"
)

type App struct {
	server *http.Server
	db     *sqlx.DB
}

func NewApp() *App {
	return &App{}
}

func connectDB() (*sqlx.DB, error) {
	// by default go sql client seems to try to connect over tcp prompting a password
	// so we need to use this brittle string
	return sqlx.Open("postgres", "postgresql:///aulty?host=/var/run/postgresql")
}

func (a *App) Init() error {
	router := mux.NewRouter()
	// TODO: use string constants for these args
	router.HandleFunc("/resolve_name",
		a.NameHandler).Methods(http.MethodGet)

	router.HandleFunc("/",
		a.Search).Methods(http.MethodGet)

	router.HandleFunc("/path_between",
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

	db, err := connectDB()
	if err != nil {
		return err
	}
	// confirm connection to db succeeded
	err = db.Ping()
	if err != nil {
		return err
	}
	a.db = db

	return nil
}

func (a *App) Run() error {
	return a.server.ListenAndServe()
}

func (a *App) Search(w http.ResponseWriter, r *http.Request) {
	// serving html search form
	t, _ := template.ParseFiles("search.gtpl")
	t.Execute(w, nil)
}

// PathBetweenHandler ...
// Usage: $ curl "http://localhost:8080/path_between?nconst+A=nm0000102&nconst+B=nm3636162"
// {film1, film2, film3...}
func (a *App) PathBetweenHandler(w http.ResponseWriter, r *http.Request) {
	nconstA := strings.Join(r.Form[nconstPathA], "")
	nconstB := strings.Join(r.Form[nconstPathB], "")
	fmt.Printf("PathBetweenHandler %s %s\n", nconstA, nconstB)
	w.Write([]byte("foo"))
}

// NameHandler handles disambiguation between textual names
func (a *App) NameHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm() //Parse url parameters passed, then parse the response packet for the POST body (request body)
	actorA := strings.Join(r.Form[actorPathA], "")
	actorB := strings.Join(r.Form[actorPathB], "")

	log.Printf("Calculating path between %s and %s\n", actorA, actorB)

	// lookup nconst for actors
	principalsA, err := principalsForName(a.db, actorA)
	// TODO: some better error handling / response for user's sake
	if err != nil {
		log.Fatal(err)
		return
	}
	principalsB, err := principalsForName(a.db, actorB)
	if err != nil {
		log.Fatal(err)
		return
	}

	// TODO: we need to provide a mechansim to resolve ambiguity via the frontend
	// Say: There are x people with name y in the database
	// Please choose one

	resp := NameResponse{
		NCONSTResp{principalsA},
		NCONSTResp{principalsB},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println(string(b))

	w.Write([]byte(b))
}
