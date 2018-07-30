package sixdegreesimdb

import (
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
	dieHardTCONST     = "tt0095016"
	dieHardName       = "Die Hard"
)

var testDB *DB

func TestMain(m *testing.M) {
	// TODO: Should we mock somehow and have tests without a requirement on the db?
	// set up db
	var err error
	testDB, err = NewDB()
	if err != nil {
		fmt.Printf("failed to init tests: %s\n", err.Error())
		os.Exit(-1)
	}
	ret := m.Run()
	os.Exit(ret)
}

func TestPrincipalsForName(t *testing.T) {
	a := assert.New(t)
	principals, err := testDB.principalsForName(bruceWillisName)
	a.NoError(err)
	a.Equal(1, len(principals))
	a.Equal(bruceWillisNCONST, principals[0].NCONST)

	// TODO: do test for an ambiguous name
	// we still need to work out how we are resolving ambiguity in our system
}

func TestPrincipalForNCONST(t *testing.T) {
	a := assert.New(t)
	principal, err := testDB.principalForNCONST(bruceWillisNCONST)
	a.NoError(err)
	a.Equal(bruceWillisName, principal.PrimaryName)
	a.Equal(bruceWillisNCONST, principal.NCONST)
}

func TestNCONSTSForTCONST(t *testing.T) {
	a := assert.New(t)
	nconsts, err := testDB.nconstsForTCONST(dieHardTCONST)
	a.NoError(err)
	a.Contains(nconsts, bruceWillisNCONST)
}

func TestTCONSTSForNCONST(t *testing.T) {
	a := assert.New(t)
	tconsts, err := testDB.tconstsForNCONST(bruceWillisNCONST)
	a.NoError(err)
	a.Contains(tconsts, dieHardTCONST)
}

func TestTitleForTCONST(t *testing.T) {
	a := assert.New(t)
	title, err := testDB.titleForTCONST(dieHardTCONST)
	a.NoError(err)
	a.Equal(dieHardName, title.PrimaryTitle)
	a.Equal(dieHardTCONST, title.TCONST)
}

func TestDoSearchNCONST(t *testing.T) {
	a := assert.New(t)
	chain, err := doSearchNCONST(testDB, bruceWillisNCONST, kevinBaconNCONST)
	a.NoError(err)
	a.Equal(2, len(chain.Links))
}
