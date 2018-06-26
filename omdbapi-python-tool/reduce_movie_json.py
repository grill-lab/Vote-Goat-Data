import json
import ujson
import csv
import re

with open('imdb_movies.json') as json_data:
        data = json.load(json_data)

count = 0
imdb_ids = []

remove_length_movies = ["N/A", "1 min", "2 min", "3 min", "4 min", "5 min", "6 min", "7 min", "8 min", "9 min", "10 min", "11 min", "12 min", "13 min", "14 min", "15 min", "16 min", "17 min", "18 min", "19 min", "20 min"]

"""
AGE RATING INFO: https://en.wikipedia.org/wiki/Motion_picture_content_rating_system
The vast majority of movies have ["N/A", "NOT RATED", "UNRATED", "NR", "Not Rated", "Not rated", "Unrated"], we do not know what kind of age rating they have, they're effectively not curated.
We do filter out 'Adult' genre, and explicit rating movies, we don't filter the above non rated movies. We could perhaps ask our users.
"""
#unwanted_ratings = ["N/A", "NOT RATED", "UNRATED", "X", "(BANNED)", "(Banned)", "BPjM Restricted", "18+", "B15", "NR", "Not Rated", "Not rated", "R-18+", "R18+", "TV-Y7", "Unrated", "VM18"] # We need to filter out inappropriate content
unwanted_ratings = ["X", "(BANNED)", "(Banned)", "BPjM Restricted", "18+", "B15", "NR", "R-18+", "R18+", "TV-Y7", "VM18"] # We need to filter out inappropriate content

filtered_movies = []
genres = []

non_filtered_movies = 0

for entry in data:
    if (entry['Response'] == "True"):
        if (entry['Genre'] != "N/A") and (entry['Genre'] != "Adult") and (entry['Runtime'] not in remove_length_movies):

            #print((entry['Rated'] not in unwanted_ratings))

            if entry['Poster'] == "N/A":
                entry['Poster'] = "https://i.imgur.com/qrMim4w.png"
            if entry['Plot'] == "N/A":
                entry['Plot'] = "Mystery movie! Watch it and find out!"
            if entry['imdbVotes'] == "N/A":
                entry['imdbVotes'] = 0
            if entry['imdbRating'] == "N/A":
                entry['imdbRating'] = 0
            if entry['Metascore'] == "N/A":
                entry['Metascore'] = 0

            output = {}
            output['title'] = entry['Title']
            output['plot'] = entry['Plot']
            output['type'] = entry['Type']
            output['genre'] = entry['Genre'].split(", ")
            output['rate_desc'] = entry['Rated']
            output['year'] = int(re.sub('[^A-Za-z0-9]+','', entry['Year'])),
            output['runtime'] = entry['Runtime']
            output['poster'] = entry['Poster']
            output['language'] = entry['Language'].split(", ")
            output['country'] = entry['Country'].split(", ")

            if hasattr(entry, 'Production'):
                output['production'] = entry['Production']
            else:
                output['production'] = "N/A"

            if hasattr(entry, 'Website'):
                output['website'] = entry['Website']
            else:
                output['website'] = "about:blank"

            output['director'] = entry['Director'].split(", ")
            output['writer'] = entry['Writer'].split(", ")
            output['actors'] = entry['Actors'].split(", ")
            output['released'] = entry['Released']

            if hasattr(entry, 'Awards'):
                output['awards'] = entry['Awards']
            else:
                output['awards'] = "N/A"

            if hasattr(entry, 'BoxOffice'):
                output['boxoffice'] = entry['BoxOffice']
            else:
                output['boxoffice'] = "N/A"

            output['imdbID'] = entry['imdbID']
            output['imdbRating'] = float((entry['imdbRating']))
            output['imdbVotes'] = int(str(entry['imdbVotes']).replace(',', ''))
            output['metascore'] = float((entry['Metascore']))
            output['ratings'] = entry['Ratings']

            genres.append(entry['Genre'].split(", "))

            non_filtered_movies += 1

            filtered_movies.append(output)
        else:
            continue
    else:
        #print("true|false: " + str(entry['Response']) + ", Type: " + str(entry['Type'] == "movie") + ", Genre N/A: " + str(entry['Genre'] != "N/A") + ", Adult?" + str(entry['Genre'] != "Adult") + ", Age: " + str(entry['Rated']))
        continue

difference = len(data) - non_filtered_movies
print("Non-Filtered: {}".format(non_filtered_movies))
print("Filtered out: {}".format(difference))

with open('filtered_movies.json', 'w') as outfile:
    ujson.dump(filtered_movies, outfile,  encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=False, indent=4)

with open("genres.csv", "w") as csv_file:
    check = 0

    flattened_genres = []

    for genre_list in genres:
        for genre in genre_list:
            flattened_genres.append(genre)

    flattened_genres = list(set(flattened_genres))

    for genre in flattened_genres:
        if (check == 0):
            csv_file.write('genre' + '\n')
        else:
            csv_file.write(genre + '\n')
        check += 1
