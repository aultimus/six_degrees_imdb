# six_degrees_imdb
six degrees of seperation implementation using imdb database


## Notes

There is too much data here for using an on disk database because of the one at a time nature of our database requests as we traverse the graph.

## TODO

* Try using a graph based database such as neo4j as your datastore, this comes with built in mechanisms to do graph queries.
* Or implement using nosql in-memory db and use our own graph search code (kind of fun to write). Graph DB is a more specific tech that doesn't apply to as many use cases as a nosql db, which experience of will be more valuable
