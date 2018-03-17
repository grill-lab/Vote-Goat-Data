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

def write_json_to_disk(filenumber, json_data, filename):
    """
    When called, write the json_data to a json file.
    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    latest_filename = filename + "_" + (str(filenumber).zfill(6)) + ".json"
    with open(latest_filename, 'w') as outfile:
        ujson.dump(json_data, outfile, encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=True) #Write JSON to data.json (disk)

def request(target_url, target_number):
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
                request(target_url) # Recursion! We don't want to skip data just because we were rate limited!
            else:
                print("ID: {} failed, Status code: {}".format(target_number, current_internal_target.status_code))
                return None
        if(current_internal_target.status_code == 200):
            return current_internal_target

def scrape_tmdb(start, stop, key, mode, append):

    widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    scrape_range_ref = (stop - start) + 1
    pbar = ProgressBar(widgets=widgets, maxval=scrape_range_ref).start() #Prepare the progress bar

    tmdb_json_data = {} #Empty data field
    tmdb_json_data['items'] = [] #Empty data list titled 'items'
    json_file_number = 0
    progress_iterator = 0
    skipped_count = 0

    for i in range(start, (stop + 1)):
        time.sleep(0.2) # Max 4 per second
        target_url = "https://api.themoviedb.org/3/" + mode + "/" + str(i) + "?api_key=" + key + "&append_to_response=" + append

        current_target = request(target_url, i) # Request the json behind the current target url

        pbar.update(progress_iterator + 1) # Display incremented progress
        progress_iterator += 1 # Iterate the progress bar for next iteration

        if (current_target == None):
            skipped_count += 1
            continue # Skip this item, it's garbage!
        else:
            tmdb_json_data['items'].append(current_target.json()) # Scrape succeeded. Store JSON.

    print("Total skipped: {}".format(skipped_count))
    write_json_to_disk(json_file_number, tmdb_json_data['items'], mode) # Final output, likely not triggered the batch limit if statement trigger above
    pbar.finish() #Once we've complete the scraping, end the progress bar.

if __name__ == '__main__':
    scrape_mode = "person" # Mode can be movie, people, etc..
    scrape_start = int(sys.argv[1]) # Start ID
    scrape_stop = int(sys.argv[2]) # End ID
    s_diff = scrape_stop - scrape_start
    API_KEY = 'API_KEY' # Private key, don't share publicly

    time_before = time.time()
    scrape_tmdb(scrape_start, scrape_stop, API_KEY, scrape_mode, "movie_credits,external_ids") # Perform the scraping!
    time_diff = time.time() - time_before

    print("Time to scrape {} records: {} seconds".format(s_diff, time_diff))
