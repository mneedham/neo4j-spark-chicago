#!/bin/sh

grep -v  "\"\"" tmp/crimesLocations.csv > tmp/crimesLocationsCleaned.csv
grep -v  "\"\"" tmp/locations.csv > tmp/locationsCleaned.csv
