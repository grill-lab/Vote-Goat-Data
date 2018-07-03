# Script to insert movie JSON into mongodb
from pymongo import *
import json

with open('filtered_movies.json') as json_data:
        imported_movie_json = json.load(json_data)

client = MongoClient("MONGODB_SEVER_DETAILS"")

db = client.votegoat

adjusted_json_list = []

for movie in imported_movie_json:
    movie['goat_upvotes'] = 0
    movie['goat_downvotes'] = 0
    movie['total_goat_votes'] = 0
    movie['sigir_upvotes'] = 0
    movie['sigir_downvotes'] = 0
    movie['total_sigir_votes'] = 0
    movie['year'] = movie['year'][0]
    adjusted_json_list.append(movie)

db.movie.insert_many(adjusted_json_list)
