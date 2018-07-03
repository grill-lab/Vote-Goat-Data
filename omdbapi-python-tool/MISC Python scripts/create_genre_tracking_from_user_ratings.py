#
from pymongo import *
import json

client = MongoClient("MONGODB_SEVER_DETAILS"")
db = client.votegoat

users = list(db.Users.find())

all_user_genre_objects = []

for user in users:
    user_genre_object = {
        "Action": {"up": 0, "down": 0},
        "Adventure": {"up": 0, "down": 0},
        "Animation": {"up": 0, "down": 0},
        "Biography": {"up": 0, "down": 0},
        "Comedy": {"up": 0, "down": 0},
        "Crime": {"up": 0, "down": 0},
        "Documentary": {"up": 0, "down": 0},
        "Drama": {"up": 0, "down": 0},
        "Family": {"up": 0, "down": 0},
        "Fantasy": {"up": 0, "down": 0},
        "Film-Noir": {"up": 0, "down": 0},
        "Horror": {"up": 0, "down": 0},
        "History": {"up": 0, "down": 0},
        "Musical": {"up": 0, "down": 0},
        "Mystery": {"up": 0, "down": 0},
        "Romance": {"up": 0, "down": 0},
        "Sci-Fi": {"up": 0, "down": 0},
        "Short": {"up": 0, "down": 0},
        "Sport": {"up": 0, "down": 0},
        "Thriller": {"up": 0, "down": 0},
        "War": {"up": 0, "down": 0},
        "Western": {"up": 0, "down": 0},
        "userId": user['userId']
    }

    all_user_genre_objects.append(json.loads(json.dumps(user_genre_object)))

#print(len(all_user_genre_objects))
db.user_genre_vote_tally.insert_many(all_user_genre_objects)
