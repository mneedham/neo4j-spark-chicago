# -*- coding: utf-8 -*-
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
ON CREATE SET
   crime.description = {params}.Description,
   crime.caseNumber  = {params}.`Case Number`,
   crime.arrest      = {params}.Arrest,
   crime.domestic    = {params}.Domestic,
   crime.fbiCode     = {params}.`FBI Code`

WITH crime
MATCH (location:Location {id: {params}.`Location Description`})
MERGE (crime)-[:COMMITTED_IN]->(location)

WITH crime
MATCH (beat:Beat {id: {params}.Beat})
MERGE (crime)-[:ON_BEAT]->(beat)
"""

tx = graph.cypher.begin()
for crime in itertools.islice(crimes, 0, 10):
    print(crime)
    tx.append(query, {"params" : crime })
    tx.process()
tx.commit()
