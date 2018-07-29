package sixdegreesimdb

import (
	"database/sql"

	"github.com/jmoiron/sqlx"

	"fmt"
)

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
	TCONST         string `db:"tconst"`
	TitleType      string `db:"titletype"`
	PrimaryTitle   string `db:"primarytitle"`
	OriginalTitle  string `db:"originaltitle"`
	IsAdult        int    `db:"isadult"`
	StartYear      int    `db:"startyear"`
	EndYear        int    `db:"endyear"`
	RuntimeMinutes int    `db:"runtimeminutes"`
	Genres         string `db:"genres"`
	NCONST         string `db:"nconst"` // should be []string?
}

// Principal represents a rich description of a principal
type Principal struct {
	NCONST            string        `db:"nconst"`
	PrimaryName       string        `db:"primaryname"`
	BirthYear         sql.NullInt64 `db:"birthyear"`
	DeathYear         sql.NullInt64 `db:"deathyear"`
	PrimaryProfession string        `db:"primaryprofession"`
	KnownForTitles    string        `db:"knownfortitles"` // should be []string?
}

func lookupName(db *sqlx.DB, name string) (*Principal, error) {
	principals, err := principalsForName(db, name)
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
func doSearchName(db *sqlx.DB, name1, name2 string) ([]Title, error) {
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
func doSearchNCONST(db *sqlx.DB, nconst1, nconst2 string) ([]Title, error) {
	principal1, err := principalForNCONST(db, nconst1)
	if err != nil {
		return nil, err
	}

	principal2, err := principalForNCONST(db, nconst2)
	if err != nil {
		return nil, err
	}
	return doSearchPrincipals(db, principal1, principal2)
}

func doSearchPrincipals(db *sqlx.DB, principal1, principal2 *Principal) ([]Title, error) {
	return []Title{}, nil
}

// TODO: should really use an ORM for this
// TODO: Handle case insensitivity?
// TODO: batching sql ops?
func principalsForName(db *sqlx.DB, name string) ([]Principal, error) {
	var principals []Principal
	err := db.Select(&principals, "SELECT * FROM name_basics WHERE primaryname = $1", name)
	return principals, err
}

func principalForNCONST(db *sqlx.DB, nconst string) (*Principal, error) {
	var principal Principal
	err := db.Get(&principal, "SELECT * FROM name_basics WHERE nconst = $1", nconst)
	return &principal, err
}
