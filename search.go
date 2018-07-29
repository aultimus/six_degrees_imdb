package sixdegreesimdb

import (
	"database/sql"
	"fmt"
)

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

// struct definitions are duplicated here as well as in the python, we could deduplicate them if we
// rewrote the python in go

// Title represents the rich description of a movie title
type Title struct {
	tconst         string
	titleType      string
	primaryTitle   string
	originalTitle  string
	isAdult        int
	startYear      int
	endYear        int
	runtimeMinutes int
	genres         string
	nconst         string // should be []string?
}

// Principal represents a rich description of a principal
type Principal struct {
	nconst            string
	primaryName       string
	birthYear         int
	deathYear         int
	primaryProfession string
	knownForTitles    string // should be []string?
}

func lookupNCONST(db *sql.DB, name string) (string, error) {
	resp, err := nconstsForName(db, name)
	if err != nil {
		return "", err
	}
	if resp.Ambiguous {
		return "", fmt.Errorf("%s is an ambiguous name", name)
	}
	return resp.IDs[0].ID, nil
}

// doSearchName given a db and two actor names searches for a path between the two names
func doSearchName(db *sql.DB, name1, name2 string) ([]Title, error) {
	nconst1, err := lookupNCONST(db, name1)
	if err != nil {
		return nil, err
	}
	nconst2, err := lookupNCONST(db, name2)
	if err != nil {
		return nil, err
	}

	return doSearchNCONST(db, nconst1, nconst2)
}

// doSearchNCONST given a db and two actor nconst values searches for a path between the two nconsts
func doSearchNCONST(db *sql.DB, nconst1, nconst2 string) ([]Title, error) {
	return []Title{}, nil
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
