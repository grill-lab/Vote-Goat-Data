# create imdb title entity csv

from pymongo import *
#import json
import csv

client = MongoClient("MONGODB_SEVER_DETAILS"")
db = client.votegoat

movie_list = list(db.movie.find())

csv_list = []
movie_titles = []

for movie in movie_list:
    if movie['title'] not in movie_titles:
        csv_list.append("\"" + movie['imdbID'] + "\", \"" + movie['imdbID'] + "\", \"" + movie['title'] + "\"" + "\n")
        movie_titles.append(movie['title'])

with open("imdb_entity.csv", "w") as csv_file:
    for line in csv_list:
        csv_file.write(line)

print(len(movie_list) - len(movie_titles))
