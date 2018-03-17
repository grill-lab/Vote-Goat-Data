# Actor data to JSON entities!
import ujson # For outputting to disk
import time # For sleeping between scrape attempts
import numpy as np # Not utilised past the failure text file function
import os # For checking the existing data files
import math # For rounding float up to nearest integer
import sys # Argument handling
import requests # JSON file downloading
import re
from progressbar import Bar, ETA, ProgressBar, Percentage # For providing progressbar functionality. This is actually "progressbar33" in python3.

def write_json_to_disk(json_data):
    """
    When called, write the json_data to a json file.
    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    with open("actor_entities.json", 'w') as outfile:
        ujson.dump(json_data, outfile, encode_html_chars=False, escape_forward_slashes=False, ensure_ascii=False) #Write JSON to data.json (disk)

def format_json(json_data):
    actor_identities = {} # Where we're going to store our list of actor ids
    actor_identities['items'] = []

    actor_quantity = len(json_data) # Get the quantity of actors!
    #actor_quantity = 2 # For testing!

    widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    scrape_range_ref = actor_quantity + 1
    pbar = ProgressBar(widgets=widgets, maxval=scrape_range_ref).start() #Prepare the progress bar
    progress_iterator = 0

    for actor_number in range(actor_quantity): # There are approx 979 pages we've scraped
        current_actor_data = json_data[actor_number] # Storing the current page in memory
        current_actor_name = current_actor_data['name'] # The actor's real name
        current_actor_roles = current_actor_data['movie_credits']['cast'] # The roles the character played
        quantity_actor_roles = len(current_actor_roles) # How many roles they played
        current_actor_role_synonyms = [] # Empty list for each actor
        current_actor_role_synonyms.append(current_actor_name) # We want the entity synonym to include their real name, not just characters:movies

        for actor_role in range(quantity_actor_roles):

            if (len(current_actor_role_synonyms) > 98):
                continue # Can't go over 99 synonyms per value

            current_target = current_actor_roles[actor_role] # Simple reference

            if 'character' not in current_target: # Make sure the character field exists!
                continue # Skip if it doesn't exist!
            else:
                if 'title' not in current_target: # Check that the title exists in json!
                    continue # Skip if the movie title doesn't exist!
                else:
                    actor_character = current_target['character'] # Grab their character name
                    if (len(actor_character) < 2):
                        continue # If the character's name is less than 2 characters long, skip (likely a blank field)
                    elif (actor_character.lower() == 'himself'):
                        continue # If they know the actor's name, they don't need to l
                    elif (actor_character.lower() == 'herself'):
                        continue
                    elif (actor_character.lower() == 'self'):
                        continue
                    else:
                        actor_character = re.sub(r'\([^()]*\)', '', actor_character) # Any parenthesised information (uncredited) (self) (himself) etc will be removed from character strings!
                        #actor_character = re.sub('".*?"', '', actor_character) # Removing any quotation marks in character name. Future improvement could match character_nickname:actor similar to handling / below
                        actor_character = re.sub('"', '', actor_character)
                        actor_movie = re.sub(r'\([^()]*\)', '', current_target['title']) # Grab the movie name

                        if ("/" in actor_character): # If the actor plays multiple characters it'll be listed as "character secret identity / character super hero"
                            multiple_characters = actor_character.split("/") # Split string into list of characters
                            for character in multiple_characters:
                                if (len(current_actor_role_synonyms) > 98):
                                    continue # Can't go over 99 synonyms per value
                                character.strip() # Remove whitespace
                                in_entity = character + " in " + actor_movie # "character in movie"
                                #from_entity = character + " from " + actor_movie # "character from movie"
                                current_actor_role_synonyms.append(in_entity) # Add the 'in' synonym
                                #current_actor_role_synonyms.append(from_entity) # Add the 'from' synonym
                        else:
                            actor_character.strip() # Strip whitespace from left/right of the string
                            in_entity = actor_character + " in " + actor_movie # 'in'
                            #from_entity = actor_character + " from " + actor_movie # 'from'
                            current_actor_role_synonyms.append(in_entity) # Add the 'in' synonym
                            #current_actor_role_synonyms.append(from_entity) # Add the 'from' synonym

        #if (len(current_actor_role_synonyms) > 95):
        #    print(len(current_actor_role_synonyms))

        text_to_json = ujson.dumps({"value": current_actor_name, "synonyms": current_actor_role_synonyms}) # Changing the text into json

        actor_identities['items'].append(ujson.decode(text_to_json)) # Append the synonyms to the list

        pbar.update(progress_iterator + 1) # Display incremented progress
        progress_iterator += 1 # Iterate the progress bar for next iteration

    pbar.finish() #Once we've complete the scraping, end the progress bar.
    return actor_identities['items']

if __name__ == '__main__':
    with open('popular_people.json') as data_file:
        actor_json_data = ujson.load(data_file) # Load actor data in

    formatted_json = format_json(actor_json_data) # Where the majority of the magic happens
    wrapped_json = ujson.decode("[{\"entries\":" + ujson.encode(formatted_json) + ", \"name\": \"actors\"}]") # Wrapping the JSON with dialogflow's preferred formatting
    write_json_to_disk(wrapped_json)
