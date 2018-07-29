package sixdegreesimdb

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

const (
	bruceWillisName   = "Bruce Willis"
	bruceWillisNCONST = "nm0000246"
	kevinBaconNCONST  = "nm0000102"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	// TODO: Should we mock somehow and have tests without a requirement on the db?
	// set up db
	var err error
	testDB, err = connectDB()
	if err != nil {
		fmt.Printf("faled to init tests: %s\n", err.Error())
		os.Exit(-1)
	}
	ret := m.Run()
	os.Exit(ret)
}

func TestNCONSTSForName(t *testing.T) {
	a := assert.New(t)
	resp, err := nconstsForName(testDB, "Bruce Willis")
	a.NoError(err)
	a.False(resp.Ambiguous)
	a.NotEmpty(resp.IDs)
	a.Equal(bruceWillisNCONST, resp.IDs[0].ID)

	// TODO: do test for an ambiguous name
	// we still need to work out how we are resolving ambiguity in our system
}

func TestDoSearchNCONST(t *testing.T) {
	a := assert.New(t)
	titles, err := doSearchNCONST(testDB, bruceWillisNCONST, kevinBaconNCONST)
	a.NoError(err)
	a.Equal([]Title{}, titles)
}

// TODO: Profile different search strategies
// BFS, DFS etc
