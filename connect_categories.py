import json
import datetime
from py2neo import Graph, authenticate

authenticate("localhost:7474", "neo4j", "neo")
graph = Graph()

query = """
MATCH (crime:Crime)
WITH crime SKIP {skip} LIMIT {limit}
MATCH (subCat:SubCategory {code: crime.fbiCode})
WITH crime, subCat, shortestPath((crime)-[:CATEGORY]->(subCat)) AS path
FOREACH(ignoreMe IN CASE WHEN path is NULL THEN [1] ELSE [] END |
  CREATE (crime)-[:CATEGORY]->(subCat))
RETURN COUNT(*) AS crimesProcessed
"""

skip = 0
limit = 10000
while True:
    start = datetime.datetime.now()
    print("Processing skip {0}, limit {1} ".format(skip, limit))
    result = graph.cypher.execute(query, skip = skip, limit = limit)
    crimes_processed = result[0]["crimesProcessed"]

    diff = datetime.datetime.now() - start
    elapsed_ms = (diff.days * 86400000) + (diff.seconds * 1000) + (diff.microseconds / 1000)

    print("Crimes processed: {0},  Time: {1}".format(crimes_processed, elapsed_ms))

    if crimes_processed == 0:
        break
    else:
        skip = skip + limit
