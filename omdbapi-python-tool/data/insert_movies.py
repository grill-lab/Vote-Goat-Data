# Script to insert movie JSON into mongodb
import pymongo
import json

with open('filtered_movies.json') as json_data:
        imported_movie_json = json.load(json_data)

client = MongoClient("MONGODB_SERVER_DETAILS")

db = client.votegoat

for movie in imported_movie_json[:2]:
    print(movie)
    #db.movie.insert_one()
