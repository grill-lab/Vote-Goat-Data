#
from pymongo import *
import json

client = MongoClient("MONGODB_SEVER_DETAILS"")
db = client.votegoat

user_ratings = list(db.user_ratings.find())

Genres = ['Action','Adventure','Animation','Biography','Comedy','Crime','Documentary','Drama','Family','Fantasy','Film-Noir','Horror', 'Music', 'Musical','Mystery', 'News', 'Romance','Sci-Fi','Short','Sport','Thriller','War','Western']
count = 0

for rating in user_ratings:
    #print(rating['userId'])
    user_genre_tallies = list(db.user_genre_vote_tally.find({'userId': rating['userId']}))
    length_user_tally = len(user_genre_tallies)

    if length_user_tally > 0:
        #print("User exists: {}".format(rating['userId']))
        # User exists
        count += 1

        genres = rating['genres']
        inc_object = {}

        for genre in genres:
            if rating['rating'] == 1:
                direction = 'up'
            else:
                direction = 'down'

            target = str(genre)+'.'+direction
            inc_object[target] = 1

        print(count)
        db.user_genre_vote_tally.update_one({"userId": rating['userId']}, {"$inc": json.loads(json.dumps(inc_object))})
    else:
        print("User didn't exist! {}".format(rating['userId']))
        count += 1
        db.user_ratings.delete_one({"_id": rating['_id']})
