DATA=${1-/tmp}
NEO=${2-./neo4j-enterprise-2.2.1}
$NEO/bin/neo4j-import \
--into $DATA/crimes.db \
--nodes $DATA/crimes.csv \
--nodes $DATA/beats.csv \
--nodes $DATA/primaryTypes.csv \
--relationships $DATA/crimesBeats.csv \
--relationships $DATA/crimesPrimaryTypes.csv
