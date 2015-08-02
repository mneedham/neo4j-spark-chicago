# -*- coding: utf-8 -*-
import json
import csv
import itertools

from py2neo import Graph, authenticate

authenticate("localhost:7474", "neo4j", "neo")
graph = Graph()

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = list(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

def new_crimes():
    with open('diff.csv') as file:
        reader = csv.DictReader(file, delimiter = ",")
        for row in reader:
            yield row

crimes = new_crimes()

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
CREATE UNIQUE (crime)-[:COMMITTED_IN]->(location)

WITH crime
MATCH (beat:Beat {id: {params}.Beat})
MERGE (crime)-[:ON_BEAT]->(beat)
"""

batch_size = 100
for chunk in grouper(batch_size, crimes):
    tx = graph.cypher.begin()
    for crime in chunk:
        tx.append(query, {"params" : crime })
        tx.process()
    tx.commit()
    print "commit"
