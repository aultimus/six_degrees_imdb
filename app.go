package sixdegreesimdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

const (
	actorPathA = "Actor A"
	actorPathB = "Actor B"
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
	router.HandleFunc("/path_between",
		a.PathBetweenHandler).Methods(http.MethodGet)

	router.HandleFunc("/",
		a.Search).Methods(http.MethodGet)

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

type NCONST struct {
	ID  string
	URL string
}

type NCONSTResp struct {
	IDs       []NCONST
	Ambiguous bool
}

type NameResponse struct {
	NCONSTa NCONSTResp
	NCONSTB NCONSTResp
}

// TODO: should really use an ORM for this
// TODO: Handle case insensitivity?

// TODO: batching sql ops?
func nconstsForName(db *sql.DB, name string) (NCONSTResp, error) {
	rows, err := db.Query("SELECT nconst FROM name_basics WHERE primaryname = $1", name)
	defer rows.Close()
	//spew.Dump(rows)
	nconst := NCONSTResp{IDs: make([]NCONST, 0, 0)}
	for rows.Next() {
		var n string
		err = rows.Scan(&n)
		if err != nil {
			return nconst, err
		}

		// TODO: format url
		nconst.IDs = append(nconst.IDs, NCONST{n, ""})
	}
	if len(nconst.IDs) > 1 {
		nconst.Ambiguous = true
	}
	return nconst, err
}

// Usage:
// $ curl "http://localhost:8080/path_between?Actor+A=Kevin+Bacon&Actor+B=George+Clooney"
// {"NCONSTa":{"IDs":[{"ID":"nm0000102","URL":""},{"ID":"nm3636162","URL":""},{"ID":"nm4025714","URL":""}],"Ambiguous":true},"NCONSTB":{"IDs":[{"ID":"nm0000123","URL":""}],"Ambiguous":false}}
// TODO: Rename this handler to NameHandler as it is responsible for
// name disambiguation / mapping rather than determining path
// have a separate handler take 2 nconsts as args and handle the search
func (a *App) PathBetweenHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm() //Parse url parameters passed, then parse the response packet for the POST body (request body)
	actorA := strings.Join(r.Form[actorPathA], "")
	actorB := strings.Join(r.Form[actorPathB], "")

	log.Printf("Calculating path between %s and %s\n", actorA, actorB)

	// lookup nconst for actors
	nconstsA, err := nconstsForName(a.db, actorA)
	// TODO: some better error handling / response for user's sake
	if err != nil {
		log.Fatal(err)
		return
	}
	nconstsB, err := nconstsForName(a.db, actorB)
	if err != nil {
		log.Fatal(err)
		return
	}
	resp := NameResponse{
		nconstsA,
		nconstsB,
	}

	// we need to provide a mechansim to resolve ambiguity via the frontend
	// Say: There are x people with name y in the database
	// Please choose one

	b, err := json.Marshal(resp)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println(string(b))

	w.Write([]byte(b))
}
