import json

with open('imdb_movies.json') as json_data:
        data = json.load(json_data)

count = 0

for entry in data:
        if entry['Response'] == "False":
                count = count + 1
                print(entry)

print(count)
