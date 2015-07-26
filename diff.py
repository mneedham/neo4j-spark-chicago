import csv
from dateutil import parser
import sys

with open("Crimes_-_2001_to_present.csv") as file1, \
     open("Crimes_-_2001_to_present.recent.csv") as file2, \
     open("diff.csv", "w") as diff:

    reader1 = csv.reader(file1, delimiter = ",")
    header = next(reader1, None)
    row = next(reader1, None)
    max_date = parser.parse(row[2])

    reader2 = csv.reader(file2, delimiter = ",")
    next(reader2, None) # skip header

    writer = csv.writer(diff, delimiter=",")
    writer.writerow(header)
    for row in reader2:
        if parser.parse(row[2]) > max_date:
            writer.writerow(row)
        else:
            sys.exit(1)
