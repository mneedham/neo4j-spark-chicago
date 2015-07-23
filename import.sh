#!/bin/sh

DATA=${1-tmp}
NEO=${2-./neo4j-community-2.2.3}
$NEO/bin/neo4j-import \
--into $DATA/crimes.db \
--nodes $DATA/crimes.csv \
--nodes $DATA/beats.csv \
--nodes $DATA/primaryTypes.csv \
--nodes $DATA/locations.csv \
--relationships $DATA/crimesBeats.csv \
--relationships $DATA/crimesPrimaryTypes.csv \
--relationships $DATA/crimesLocationsCleaned.csv \
--stacktrace
