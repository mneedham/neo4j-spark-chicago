import json
from py2neo import Graph, authenticate

authenticate("localhost:7474", "neo4j", "neo")
graph = Graph()

with open('categories.json') as data_file:
    json = json.load(data_file)

query = """
WITH {json} AS document
UNWIND document.categories AS category
UNWIND category.sub_categories AS subCategory
MERGE (c:CrimeCategory {name: category.name})
MERGE (sc:SubCategory {code: subCategory.code})
ON CREATE SET sc.description = subCategory.description
MERGE (c)-[:CHILD]->(sc)
"""

print graph.cypher.execute(query, json = json)
