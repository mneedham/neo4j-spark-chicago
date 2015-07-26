// crimes
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MATCH (crime:Crime {id: row.ID})
SET crime.fbiCode = row.`FBI Code`;

CREATE INDEX ON :SubCategory(code);

MATCH (crime:Crime)
MATCH (subCat:SubCategory {code: crime.fbiCode})
MERGE (crime)-[:CATEGORY]->(subCat);
