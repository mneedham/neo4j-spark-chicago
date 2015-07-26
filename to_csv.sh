#!/bin/sh

echo "category,sub_category_code,sub_category_description" > categories.csv
jq -r ".categories[] |
       {name: .name, sub_category: .sub_categories[]} |
       [.name, .sub_category.code, .sub_category.description] |
       @csv " categories.json >> categories.csv
