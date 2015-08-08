#!/bin/sh

time ./spark-1.3.0-bin-hadoop1/bin/spark-submit \
--packages com.databricks:spark-csv_2.10:1.1.0 \
--driver-memory 5g \
--class GenerateCSVFiles \
--master local[8] \
target/scala-2.10/playground_2.10-1.0.jar \
Crimes_-_2001_to_present.csv
