#!/bin/sh

./spark-1.1.0-bin-hadoop1/bin/spark-submit --driver-memory 5g --class CrimeApp3 --master local[8] target/scala-2.10/playground_2.10-1.0.jar
