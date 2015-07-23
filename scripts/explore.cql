// first row of the file
LOAD CSV WITH HEADERS FROM
"file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
RETURN row
LIMIT 1

// check for the number of crimes of a certain type?
MATCH (location:Location {id: "RESTAURANT"})<-[:COMMITTED_IN]-(crime)
RETURN COUNT(*)

MATCH (location:Location {id: "RESTAURANT"})<-[c:COMMITTED_IN]-(crime)
RETURN *
LIMIT 10

// a crime
match (c:Crime {id: "9457526"})
RETURN c
