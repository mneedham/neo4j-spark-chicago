// index all the things
CREATE INDEX ON :Crime(id);
CREATE INDEX ON :Location(name);
CREATE INDEX ON :CrimeType(name);
CREATE INDEX ON :Location(name);

// crimes
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MERGE (crime:Crime {id: row.ID,
                    description: row.Description,
                    caseNumber: row.`Case Number`,
                    arrest: row.Arrest,
                    domestic: row.Domestic});
                    
// crime types
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MERGE (:CrimeType {crimeType: row.`Primary Type` });

// beats
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MERGE (:Beat {id: row.Beat });

// location
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000 WHERE row.`Location Description` <> ""
MERGE (:Location {id: row.`Location Description`});

// crimes -> location
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MATCH (crime:Crime {id: row.ID, description: row.Description})
MATCH (location:Location {id: row.`Location Description`})
MERGE (crime)-[:COMMITTED_IN]->(location);

// crimes -> beats
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MATCH (crime:Crime {id: row.ID, description: row.Description})
MATCH (beat:Beat {id: row.Beat})
MERGE (crime)-[:ON_BEAT]->(beat);

// crimes -> crime type
using periodic commit
load csv with headers from "file:///Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv" AS row
WITH row LIMIT 10000
MATCH (crime:Crime {id: row.ID, description: row.Description})
MATCH (crimeType:CrimeType {crimeType: row.`Primary Type`})
MERGE (crime)-[:TYPE]->(crimeType);
