#import tmdbsimple as tmdb
#tmdb.

import ujson # For outputting to disk
import time # For sleeping between scrape attempts
import numpy as np # Not utilised past the failure text file function
import os # For checking the existing data files
import math # For rounding float up to nearest integer
import sys # Argument handling
import requests # JSON file downloading
from progressbar import Bar, ETA, ProgressBar, Percentage # For providing progressbar functionality. This is actually "progressbar33" in python3.

def write_json_to_disk(json_data, mode):
    """
    When called, write the json_data to a json file.

    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    if (mode == "person"):
        latest_filename = "popular_people.json"
    else:
        latest_filename = "popular_movies.json"

    with open(latest_filename, 'w') as outfile:
        ujson.dump(json_data, outfile, encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=True) #Write JSON to data.json (disk)

def request(target_url, target_number):
    """
    Request target url json data.
    Checks for valid request result, automatically handles
    """
    try:
        current_internal_target = requests.get(target_url, headers = {'User-agent': 'POPCORN GOOGLE ASSISTANT BOT v0.01'})
    except:
        print("ID: {} failed to download!".format(target_number))
        return None
    if(current_internal_target.status_code != 200): #Check if the scraped data contains an error (such as exceeding the quantity of their database's contents)
        if(current_internal_target.status_code == 429):
            time_to_sleep = current_internal_target.headers['Retry-After'] + 0.5
            print("Rate limited! Waiting: {} seconds".format(str(time_to_sleep)))
            time.sleep(time_to_sleep)
            request(target_url, target_number) # Recursion! We don't want to skip data just because we were rate limited!
        else:
            print("ID: {} failed, Status code: {}".format(target_number, current_internal_target.status_code))
            return None
    if(current_internal_target.status_code == 200):
        return current_internal_target

def scrape_tmdb(popular_list, key, mode, append):
    """
    Scrape tmdb json data, includes progress bar logic.
    """
    widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    scrape_range_ref = len(popular_list) + 1
    pbar = ProgressBar(widgets=widgets, maxval=scrape_range_ref).start() #Prepare the progress bar

    tmdb_json_data = {} #Empty data field
    tmdb_json_data['items'] = [] #Empty data list titled 'items'
    progress_iterator = 0
    skipped_count = 0

    for celebrity in popular_list:
        time.sleep(0.2) # Max 4 per second
        target_url = "https://api.themoviedb.org/3/" + mode + "/" + str(celebrity) + "?api_key=" + key + "&append_to_response=" + append

        current_target = request(target_url, celebrity) # Request the json behind the current target url

        pbar.update(progress_iterator + 1) # Display incremented progress
        progress_iterator += 1 # Iterate the progress bar for next iteration

        if (current_target == None):
            skipped_count += 1
            continue # Skip this item, it's garbage!
        else:
            tmdb_json_data['items'].append(current_target.json()) # Scrape succeeded. Store JSON.

    print("Total skipped: {}".format(skipped_count))
    write_json_to_disk(tmdb_json_data['items'], mode) # Final output, likely not triggered the batch limit if statement trigger above
    pbar.finish() #Once we've complete the scraping, end the progress bar.

def scrape_popular_tmdb(key, pages):

    json_data = {} #Empty data field
    json_data['items'] = [] #Empty data list titled 'items'

    popular_widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    popular_bar = ProgressBar(widgets=popular_widgets, maxval=pages).start() #Prepare the progress bar
    progress_iterator = 0

    for page_number in range(pages + 1):
        if (page_number == 0):
            continue

        time.sleep(0.2) # Max 4 per second

        target_url = "https://api.themoviedb.org/3/person/popular?api_key=" + key + "&language=en-US&page=" + str(page_number)

        current_target = request(target_url, page_number) # Request the json behind the current target url

        popular_bar.update(progress_iterator + 1) # Display incremented progress
        progress_iterator += 1 # Iterate the progress bar for next iteration

        if (current_target == None):
            continue # Skip this item, it's garbage!
        else:
            json_data['items'].append(current_target.json()) # Scrape succeeded. Store JSON.

    popular_bar.finish() #Once we've complete the scraping, end the progress bar.
    return json_data['items']

def popular_json_to_list(actor_data, page_quantity):
    actor_identities = [] # Where we're going to store our list of actor ids

    for page_number in range(page_quantity): # There are approx 979 pages we've scraped
        current_page_data = actor_data[page_number]['results'] # Storing the current page in memory
        scrape_range = len(current_page_data) # The final page isn't always 20 in length, thus we must adjust for the length of each page's results.

        for actor_number in range(scrape_range): # Each page has approx 20 results
            #print("page_number: {}, actor_number: {}".format(page_number, actor_number))
            current_target = current_page_data[actor_number]['id'] # Access the actors within the current page in memory
            actor_identities.append(current_target) # Append the ID to our actor identity list
    return actor_identities # Return the list

if __name__ == '__main__':
    scrape_mode = "person" # Mode can be movie, people, etc..
    API_KEY = 'API_KEY' # Private key, don't share publicly
    pages = 979
    #popular_actors = scrape_popular_tmdb(API_KEY, 980)
    print("Part 1 start")
    popular_actors = scrape_popular_tmdb(API_KEY, pages)
    print("Part 2 start")
    popular_actor_ids = popular_json_to_list(popular_actors, pages)
    print("Part 3 start")
    time_before = time.time()
    scrape_tmdb(popular_actor_ids, API_KEY, scrape_mode, "movie_credits,external_ids") # Perform the scraping!
    time_diff = time.time() - time_before

    #print("Time to scrape {} records: {} seconds".format(s_diff, time_diff))
