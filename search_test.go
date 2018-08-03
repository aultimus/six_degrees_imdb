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
	alanRickmanNCONST = "nm0000614"
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
	nconsts, err := testDB.nconstsForTCONST(dieHardTCONST, alanRickmanNCONST)
	a.NoError(err)
	a.Contains(nconsts, bruceWillisNCONST)
	a.NotContains(nconsts, alanRickmanNCONST)
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

//func TestDoSearchNCONST(t *testing.T) {
//	a := assert.New(t)
//	chain, err := doSearchNCONST(testDB, bruceWillisNCONST, kevinBaconNCONST)
//	a.NoError(err)
//	a.Equal(2, len(chain.Links))
//}

func TestNodeEquality(t *testing.T) {
	a := assert.New(t)
	n1 := NewNode(&Data{"foo"})
	n2 := NewNode(&Data{"foo"})

	a.True(n1.Equal(n2))
	a.True(n2.Equal(n1))

	n3 := NewNode(&Data{"bar"})
	a.False(n1.Equal(n3))
	a.False(n2.Equal(n3))
	a.False(n3.Equal(n1))
	a.False(n3.Equal(n2))
}

func TestBFS(t *testing.T) {
	a := assert.New(t)
	g := NewGraph(testDB)

	start := g.GetNode(bruceWillisNCONST)
	goal := g.GetNode(kevinBaconNCONST)
	path := g.bfs(start, goal)
	a.Equal(2, arrToLen(path))
}

func TestGoalToArr(t *testing.T) {
	a := assert.New(t)

	n := NewNode(&Data{"ncat"})
	n.prev = &Edge{&Data{"tbar"}, nil, nil}
	n.prev.prev = NewNode(&Data{"nfoo"})

	arr := goalToArr(n)
	a.Equal([]string{"nfoo", "tbar", "ncat"}, arr)
}

// BenchmarkAdj-4   	       1	3905898174 ns/op	  205320 B/op	    5765 allocs/op
func BenchmarkAdj(b *testing.B) {
	for n := 0; n < b.N; n++ {
		g := NewGraph(testDB) //  will sql cache results for us?
		n := g.GetNode(bruceWillisNCONST)
		g.Adj(n, nil)
	}
}

// BenchmarkTCONSTForNCONST-4   	       1	3782539178 ns/op	   42032 B/op	     621 allocs/op
func BenchmarkTCONSTForNCONST(b *testing.B) {
	for n := 0; n < b.N; n++ {
		testDB.tconstsForNCONST(bruceWillisNCONST)
	}
}

// BenchmarkNCONSTForTCONST-4   	       1	3710530782 ns/op	   30752 B/op	     238 allocs/op
func BenchmarkNCONSTForTCONST(b *testing.B) {
	for n := 0; n < b.N; n++ {
		testDB.tconstsForNCONST(dieHardTCONST)
	}
}
