package sixdegreesimdb

import (
	"database/sql"
	"strings"

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

// bidirectional bfs is probably the most efficient algorithm but simple bfs is probably
// easier to implement first

// we can't load the whole graph into memory at once so let's retrieve the data to add nodes upon
// request

func NewGraph(db *DB) *Graph {
	return &Graph{make(map[string]*Node), make([]*Node, 0, 0), db}
}

type Graph struct {
	nodeMap map[string]*Node
	queue   []*Node
	db      *DB
}

func NewNode(d *Data) *Node {
	//fmt.Printf("created node %s\n", d.val)
	return &Node{data: d}
}

func (g *Graph) GetNode(id string) *Node {
	n, exists := g.nodeMap[id]
	if !exists {
		n = NewNode(&Data{id})
	}
	return n
}

func (g *Graph) AddNode(n *Node) {
}

// data for edges is tconst
// data for nodes is nconst

// should return something more informative than bool in future like a Node or Data list
func (g *Graph) bfs(start, goal *Node) []string {
	g.queue = append(g.queue, start)
	start.visited = true

	var v *Node
	for len(g.queue) != 0 {
		v, g.queue = g.queue[0], g.queue[1:] // pop v
		g.nodeMap[v.data.val] = v
		fmt.Printf("processing node %s, node count %d\n", v.data.val, len(g.nodeMap))
		if v.Equal(goal) {
			fmt.Println("found path")
			return goalToArr(v)
		}

		// all neighbours of v
		adj := g.Adj(v)
		fmt.Printf("node has %d adjacent nodes\n", len(adj))
		for _, w := range adj {
			if !w.visited {
				g.queue = append(g.queue, w)
				w.visited = true
			}
		}
	}
	return []string{}
}

func (g *Graph) Adj(n *Node) []*Node {
	var nodes []*Node

	// Find all movies our actor is in
	tconsts, err := g.db.tconstsForNCONST(n.data.val)
	if err != nil {
		// should handle error more gracefully than this
		panic(err.Error())
	}

	// Find all actors that are in the same movies as our actor
	for _, tconst := range tconsts {
		nconsts, err := g.db.nconstsForTCONST(tconst)
		if err != nil {
			// should handle error more gracefully than this
			panic(err.Error())
		}

		// add each of our new nodes
		for _, nconst := range nconsts {
			n1 := g.GetNode(nconst)
			e := &Edge{&Data{tconst}, n, n1}
			n1.prev = e
			nodes = append(nodes, n1)
		}
	}
	return nodes
}

func goalToArr(n *Node) []string {
	arr := make([]string, 0, 0)
	for {
		arr = append([]string{n.data.String()}, arr...)
		if n.prev == nil {
			return arr
		}
		arr = append([]string{n.prev.data.String()}, arr...)
		n = n.prev.prev
	}
	return arr
}

func arrToLen(arr []string) int {
	var count int
	for _, v := range arr {
		if len(v) > 0 && strings.HasPrefix(v, "t") {
			count++
		}
	}
	return count
}

//Data a basic data structure associated with our graph nodes for now (can become more complex later)
type Data struct {
	val string
}

func (d *Data) String() string {
	return d.val
}

type Dataer interface {
	Data() string
}

// Actors are the nodes and the films are the edges that connect the actors
type Node struct {
	// data
	data *Data
	//
	visited bool
	prev    *Edge
}

//func (n *Node) Add(new *Node) {
//	n.next = new
//}

func (n *Node) AddEdge(n2 *Node, tconst string) {
	e := &Edge{&Data{tconst}, n, n2}
	//n.next = e
	n.prev = e
}

//func (n *Node) AreAdjacent(w *Node) bool {
//	if n.prev != nil {
//		if n.prev.prev == w {
//			return true
//		}
//	}
//	if w.prev != nil {
//		if w.prev.prev == n {
//			return true
//		}
//	}
//	return false
//}

func (n *Node) Equal(other *Node) bool {
	// the goal state is a special case as it does not
	return *n.data == *other.data
}

type Edge struct {
	data *Data
	prev *Node
	next *Node
}

// adjacency definition:
// an adjacent node is a different actor in the same movie (different nconst, same tconst)
// or the same actor in a different movie (same nconst, different tconst)

func findCommonElement(a, b []string) (bool, string) {
	for i := 0; i < len(a); i++ {
		for j := 0; j < len(b); j++ {
			if a[i] == b[j] {
				return true, a[i]
			}
		}
	}
	return false, ""
}

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
	err := db.db.Select(&nconsts, "SELECT nconst FROM title_principals WHERE tconst = $1 AND (category='actor' OR category='actress')", tconst)
	return nconsts, err
}

// given an nconst find all tconst - table title_principals
func (db *DB) tconstsForNCONST(nconst string) ([]string, error) {
	var tconsts []string
	// if we add title_type onto title_principals table then we can get away without doing a join which may be an optimisation
	err := db.db.Select(&tconsts, "SELECT title_principals.tconst FROM title_principals INNER JOIN title_basics ON title_principals.tconst = title_basics.tconst WHERE nconst = $1 AND titletype = 'movie'", nconst)
	return tconsts, err
}

func (db *DB) titleForTCONST(tconst string) (Title, error) {
	var title Title
	err := db.db.Get(&title, "SELECT * FROM title_basics WHERE tconst = $1", tconst)
	return title, err
}
