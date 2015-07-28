#!/bin/sh

./neo4j-community-2.2.3/bin/neo4j stop
rm -rf neo4j-community-2.2.3/data/crimes.db/
cp -r tmp/crimes.db neo4j-community-2.2.3/data/
./neo4j-community-2.2.3/bin/neo4j start
python import_categories.py
./neo4j-community-2.2.3/bin/neo4j-shell --file scripts/indexes.cql
