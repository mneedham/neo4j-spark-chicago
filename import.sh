#!/bin/sh

DATA=${1-tmp}
NEO=${2-./neo4j-enterprise-2.2.3}
$NEO/bin/neo4j-import \
--into $DATA/crimes.db \
--nodes $DATA/crimes.csv \
--nodes $DATA/beats.csv \
--nodes $DATA/primaryTypes.csv \
--nodes $DATA/locationsCleaned.csv \
--relationships $DATA/crimesBeats.csv \
--relationships $DATA/crimesPrimaryTypes.csv \
--relationships $DATA/crimesLocationsCleaned.csv \
--stacktrace
