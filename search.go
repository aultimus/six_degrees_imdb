package sixdegreesimdb

import (
	"database/sql"

	"github.com/jmoiron/sqlx"

	"fmt"
)

// Note that new imdb data sets now replace the old peculiarly formatted .list files. The .list
// files had a horrible format and were extremely brittle and difficult to parse lacking fixed keys
// but were at least exhaustive. They are now no longer updated in favour of the new format. I
// previously tried to parse the old format at the start of this project with great frustration and
// difficulty.

// The new format (TSV) whilst sane and receiving updates is non-exhaustive, thus using this data,
// we cannot hope to achieve the same results as say oracleofbacon.org

// old: ftp://ftp.funet.fi/pub/mirrors/ftp.imdb.com/pub/
// new: https://datasets.imdbws.com/
// new format documentation: https://www.imdb.com/interfaces/
// discussion: https://getsatisfaction.com/imdb/topics/imdb-data-now-available-in-amazon-s3

type NCONST struct {
	ID  string
	URL string
}

type NCONSTResp struct {
	Principals []Principal
	//Ambiguous  bool
}

type NameResponse struct {
	NCONSTa NCONSTResp
	NCONSTB NCONSTResp
}

// struct definitions are duplicated here as well as in the python, we could deduplicate them if we
// rewrote the python in go

// Title represents the rich description of a movie title
type Title struct {
	TCONST         string        `db:"tconst"`
	TitleType      string        `db:"titletype"`
	PrimaryTitle   string        `db:"primarytitle"`
	OriginalTitle  string        `db:"originaltitle"`
	IsAdult        int           `db:"isadult"`
	StartYear      sql.NullInt64 `db:"startyear"`
	EndYear        sql.NullInt64 `db:"endyear"`
	RuntimeMinutes int           `db:"runtimeminutes"`
	Genres         string        `db:"genres"`
	NCONST         string        `db:"nconst"` // should be []string?
}

type Role struct {
	Ordering   int    `db:"ordering"`
	NCONST     string `db:"nconst"`
	Category   string `db:"category"`
	Job        string `db:"job"`
	Characters string `db:"characters"`
}

// Principal represents a rich description of a principal
type Principal struct {
	NCONST            string        `db:"nconst"`
	PrimaryName       string        `db:"primaryname"`
	BirthYear         sql.NullInt64 `db:"birthyear"`
	DeathYear         sql.NullInt64 `db:"deathyear"`
	PrimaryProfession string        `db:"primaryprofession"`
	KnownForTitles    string        `db:"knownfortitles"` // kind of pointless field (non-exhaustive). should be []string?
	titles            []Title
}

// Link represents an element in the chain between two principals. A Link signifies that principal
// was in title
type Link struct {
	Title     *Title
	Principal *Principal
}

// Chain represents a result in
// TODO: Optimise to use less data and then retrieve that data when we want to display a chain?
type Chain struct {
	Start *Principal
	End   *Principal
	Links []Link
}

func newChain(principal1, principal2 *Principal) *Chain {
	return &Chain{
		Start: principal1,
		End:   principal2,
		Links: make([]Link, 0, 0),
	}
}

// TODO: we shouldn't be passing in a literal *sqlx.DB here, we should be using some abstraction to
// enable mocking
func lookupName(db *DB, name string) (*Principal, error) {
	principals, err := db.principalsForName(name)
	if err != nil {
		return &Principal{}, err
	}
	if len(principals) > 1 {
		return &Principal{}, fmt.Errorf("%s is an ambiguous name", name)
	}
	return &principals[0], nil
}

// doSearchName given a db and two actor names searches for a path between the two names
// TODO: Limit to only actors for now to put off rhe need for resolving name disambiguation
func doSearchName(db *DB, name1, name2 string) (*Chain, error) {
	principal1, err := lookupName(db, name1)
	if err != nil {
		return nil, err
	}
	principal2, err := lookupName(db, name2)
	if err != nil {
		return nil, err
	}

	return doSearchPrincipals(db, principal1, principal2)
}

// doSearchNCONST given a db and two actor nconst values searches for a path between the two nconsts
func doSearchNCONST(db *DB, nconst1, nconst2 string) (*Chain, error) {
	principal1, err := db.principalForNCONST(nconst1)
	if err != nil {
		return nil, err
	}

	principal2, err := db.principalForNCONST(nconst2)
	if err != nil {
		return nil, err
	}
	return doSearchPrincipals(db, principal1, principal2)
}

// BFS
// 1.given nconstA find all tconst it is in
// 2. check if nconstB is in any of these tconst
// 3. if so return the chain
// 4. find all the nconstX in these tconst
// 5. find all the tconst that nconstX is in
// 6. GOTO 2

func doSearchPrincipals(db *DB, principal1, principal2 *Principal) (*Chain, error) {
	return newChain(principal1, principal2), nil
}

func NewDB() (*DB, error) {
	db, err := connectDB()
	return &DB{db}, err
}

type DB struct {
	db *sqlx.DB
}

// TODO: Move db specific code into its own file or package away from general search algorithms

// TODO: should really use an ORM for this
// TODO: Handle case insensitivity?
// TODO: batching sql ops?
func (db *DB) principalsForName(name string) ([]Principal, error) {
	var principals []Principal
	err := db.db.Select(&principals, "SELECT * FROM name_basics WHERE primaryname = $1", name)
	return principals, err
}

func (db *DB) principalForNCONST(nconst string) (*Principal, error) {
	var principal Principal
	err := db.db.Get(&principal, "SELECT * FROM name_basics WHERE nconst = $1", nconst)
	return &principal, err
}

// given a tconst find all nconst - table title_principals
func (db *DB) nconstsForTCONST(tconst string) ([]string, error) {
	var nconsts []string
	err := db.db.Select(&nconsts, "SELECT nconst FROM title_principals WHERE tconst = $1", tconst)
	return nconsts, err
}

// given an nconst find all tconst - table title_principals
func (db *DB) tconstsForNCONST(nconst string) ([]string, error) {
	var tconsts []string
	err := db.db.Select(&tconsts, "SELECT tconst FROM title_principals WHERE nconst = $1", nconst)
	return tconsts, err
}

func (db *DB) titleForTCONST(tconst string) (Title, error) {
	var title Title
	err := db.db.Get(&title, "SELECT * FROM title_basics WHERE tconst = $1", tconst)
	return title, err
}
