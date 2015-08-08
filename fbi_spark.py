from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

# file = "/Users/markneedham/projects/neo4j-spark-chicago/Crimes_-_2001_to_present.csv"
file = "hdfs://localhost:9000/user/markneedham/Crimes_-_2001_to_present.csv"


sqlContext.load(source="com.databricks.spark.csv", header="true", path = file).registerTempTable("crimes")
rows = sqlContext.sql("select `FBI Code` AS fbiCode, COUNT(*) AS times FROM crimes GROUP BY `FBI Code` ORDER BY times DESC").collect()

for row in rows:
    print("{0} -> {1}".format(row.fbiCode, row.times))

# import csv
# from collections import Counter
#
# cnt = Counter()
# with open("Crimes_-_2001_to_present.csv") as file1:
#     reader1 = csv.reader(file1, delimiter = ",")
#     header = next(reader1, None)
#
#     for row in reader1:
#         cnt[row[14]] += 1
#
# print cnt
