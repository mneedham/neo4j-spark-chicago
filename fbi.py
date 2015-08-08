import csv
from collections import Counter

cnt = Counter()
# with open("Crimes_-_2001_to_present.csv") as file1:
with open("Crimes_-_2001_to_present.recent.csv") as file1:
    reader1 = csv.reader(file1, delimiter = ",")
    header = next(reader1, None)

    for row in reader1:
        cnt[row[14]] += 1

print cnt
