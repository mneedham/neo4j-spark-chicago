import json
from py2neo import Graph, authenticate

authenticate("localhost:7474", "neo4j", "neo")
graph = Graph()

query = """
MATCH (crime:Crime)
WITH crime SKIP {skip} LIMIT {limit}
MATCH (subCat:SubCategory {code: crime.fbiCode})
MERGE (crime)-[:CATEGORY]->(subCat)
RETURN COUNT(*) AS crimesProcessed
"""

skip = 0
limit = 1000
while True:
    print("Processing skip {0}, limit {1} ".format(skip, limit))
    result = graph.cypher.execute(query, skip = skip, limit = limit)
    crimes_processed = result[0]["crimesProcessed"]

    if crimes_processed == 0:
        break
    else:
        skip = skip + limit
