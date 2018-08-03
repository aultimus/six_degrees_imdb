package sixdegreesimdb

import (
	"strings"

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

// These data structures are used by the web front end which is currently not in use and haven't
// settled yet
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

func NewGraph(db Databaser) *Graph {
	return &Graph{make(map[string]*Node), make([]*Node, 0, 0), db}
}

type Graph struct {
	nodeMap map[string]*Node
	queue   []*Node
	db      Databaser
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

// data for edges is tconst
// data for nodes is nconst

// database on disk is too slow, for a simple request, some actors have connectedness of 200 meaning
// that we have to do 200 queries / accesses in the worst case for an actor in the same movie
//  we need some in memory storage

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
		adj := g.Adj(v, goal)
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

func (g *Graph) Adj(n, goal *Node) []*Node {
	var nodes []*Node

	// Find all movies our actor is in
	tconsts, err := g.db.tconstsForNCONST(n.data.val)
	if err != nil {
		// should handle error more gracefully than this
		panic(err.Error())
	}

	// Find all actors that are in the same movies as our actor
	for _, tconst := range tconsts {
		nconsts, err := g.db.nconstsForTCONST(tconst, n.data.val)
		if err != nil {
			// should handle error more gracefully than this
			panic(err.Error())
		}

		// add each of our new nodes
		for _, nconst := range nconsts {
			n1 := g.GetNode(nconst)
			e := &Edge{&Data{tconst}, n, nil}
			n1.prev = e
			if n1.Equal(goal) { // quick shortcut avoiding processing other nodes of same depth
				return []*Node{n1}
			}
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

//func (n *Node) AddEdge(n2 *Node, tconst string) {
//	e := &Edge{&Data{tconst}, n, n2}
//	//n.next = e
//	n.prev = e
//}

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

type Databaser interface {
	nconstsForTCONST(tconst, excludeNCONST string) ([]string, error)
	tconstsForNCONST(nconst string) ([]string, error)
}
