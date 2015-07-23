import re
import json

from bs4 import BeautifulSoup
from soupselect import select

page = open("crime_types.html", "r").read()
soup = BeautifulSoup(page, 'html.parser')

crimes = []
for row in select(soup, "table tr")[3:6]:
    columns = select(row, "td")
    category = select(columns[0], "span")[0].text

    sub_categories = [{"description": item.group(1), "code": item.group(2)}
                       for item in
                         [re.search("(.*) \((.*)\)$", raw_sub_category)
                          for raw_sub_category in
                            [item.text.replace("(Index)", "").replace(u'\xa0', u' ').strip()
                             for item in select(columns[1], "span a")
                            ]
                         ]
                      ]

    crimes.append({"name": category, "sub_categories": sub_categories})

print json.dumps(crimes)

with open('categories.json', 'w') as outfile:
    json.dump({"categories": crimes}, outfile, indent=4)
