import json
import csv

with open('imdb_movies.json') as json_data:
        data = json.load(json_data)

count = 0
imdb_ids = []

for entry in data:
        if entry['Response'] == "False":
            imdb_ids.append(entry['imdb_id'])

with open("failed_imdb_ids.csv", "w") as csv_file:
    check = 0
    for id in imdb_ids:
        if (check == 0):
            csv_file.write('id' + '\n')
        else:
            csv_file.write(id + '\n')
        check += 1
