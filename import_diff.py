import json
import csv
import itertools

from py2neo import Graph, authenticate

authenticate("localhost:7474", "neo4j", "neo")
graph = Graph()

def new_crimes():
    with open('diff.csv') as file:
        reader = csv.DictReader(file, delimiter = ",")
        for row in reader:
            yield row

crimes = new_crimes()

# update this with the full query
query = """
MERGE (crime:Crime {id: {params}.`ID`})
"""

# loop through each crime and commit a transaction for each one
# next we can batch these and see if it makes any difference
#  how to measure?
for crime in crimes:
    tx = graph.cypher.begin()
    tx.append(query, {"params" : crime })
    tx.commit()


# query = """
# WITH {json} AS document
# UNWIND document.categories AS category
# UNWIND category.sub_categories AS subCategory
# MERGE (c:CrimeCategory {name: category.name})
# MERGE (sc:SubCategory {code: subCategory.code})
# ON CREATE SET sc.description = subCategory.description
# MERGE (c)-[:CHILD]->(sc)
# """
#
# print graph.cypher.execute(query, json = json)
