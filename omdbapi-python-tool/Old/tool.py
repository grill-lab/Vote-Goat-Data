import omdb # For scraping omdbapi.com
import ujson # For outputting to disk
from time import sleep # For sleeping between scrape attempts
import numpy as np # Not utilised past the failure text file function
import os # For checking the existing data files
import math # For rounding float up to nearest integer
import sys # Argument handling

import pandas # For tsv parsing

from progressbar import Bar, ETA, ProgressBar, Percentage # For providing progressbar functionality. This is actually "progressbar33" in python3.

def del_file(file_number):
    """
    Delete a json file. Perhaps a def is overboard.
    """
    latest_file_name = 'data_' + (str(file_number).zfill(6)) + '.json'
    os.remove(latest_file_name)

def check_json_files(range_ref):
    """
    Check for the existence of existing data_xxxxxx.json files.
    If they do exist, delete the final data file (in case it is partially filled, or broken).
    Return final data file - 1.
    Purpose: Recover last known scraping state, avoiding manually finding the latest scrape_start value to tweak.
    """
    no_file = None # Perhaps we could just return 'None' instead of declaring ahead of the if statement?

    if(os.path.exists('data_000000.json')):
        latest_json_file = 0 # Init the default value

        ceiling_range = int(math.ceil(range_ref/json_batch_limit)) + 1 # Could be float, so round up to integer!
        #print("ceiling_range: {}".format(ceiling_range))
        for i in range(ceiling_range):
            #print("i: {}".format(i))
            json_file_name = 'data_' + (str(i).zfill(6)) + '.json'
            if(os.path.exists(json_file_name)):
                #print("continue: {}".format(i))
                latest_json_file = i
                continue
            else:
                #print("break: {}".format(i))
                break # We found the latest!
        #print("deleting: {}".format(latest_json_file))
        #print("delete: {}".format(i))
        del_file(latest_json_file) # Delete the final file to avoid issues
        #return (latest_json_file - 1) # Return the last remaining file number (after we deleted the latest file)
    else:
        return no_file

def generate_imdb_tt_list(start, stop):
    """
    Generate the list of IMDB id tags, which we will use to query omdbapi.
    Format: "tt + (fill: up to 6 zeros) number"
    """

    imdb_ids = []

    with open('title.akas.tsv') as f:
        table = pandas.read_table(f)
        imdb_ids = table['titleId'].tolist()

    imdb_tag_list = [] # Empty list to hold our imdb tags

    for current_iteration in range(start, stop):
        if current_iteration == 0:
            continue #Skipping 0 because there is no 0 entry
        prepend_imdb_tag = 'tt' #Required prepended tag
        imdb_tag_list.append(prepend_imdb_tag+(str(current_iteration).zfill(7))) #appending the number to tt & padding with zeros.
        #print("generate_imdb {}".format(imdb_tag_list[-1]))

    return imdb_ids # Return the id list

def write_json_to_disk(filenumber, json_data):
    """
    When called, write the json_data to a json file.
    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    latest_filename = 'data_' + (str(filenumber).zfill(7)) + '.json'
    with open(latest_filename, 'w') as outfile:
        ujson.dump(json_data, outfile, encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=True) #Write JSON to data.json (disk)

def write_failures_to_disk(failure_data):
    """
    When called will write all timeout'd query IMDB ids.
    Future functionality could include these files
    """
    if (np.size(failure_data) > 0):
        with open("failure.txt", "w") as text_file:
            for skipped_item in failure_data:
                text_file.write(skipped_item + '\n')

def scrape_omdb(scrape_range_ref, omdb_id_list, json_file_number):
    """
    When called, will scrape omdbapi for the range of imdb ids we generated during the generate_imdb_tt_list() step.
    Will display a progress bar so you know the script is still functioning.
    WARNING: If you set the range to 1 million records, expect the script to take up to 2-3 days.
    """

    widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    pbar = ProgressBar(widgets=widgets, maxval=scrape_range_ref).start() #Prepare the progress bar

    skipped_ids = [] # Initializing a list to keep track of scraping attempts which timed out.
    imdb_json_data = [] # Where we'll store the JSON response in memory before outputting to disk

    progress_iterator = 0 #For the progress bar
    json_file_number = 0 # Start with "data_000000.json"
    json_batch_iterator = 0 #Every json_batch_iterator iterations we will revert to 0, limiting the size of each json file.

    for current_tag in omdb_id_list: #for loop iterate over the list of imdb tags we generated
        sleep(0.05) #Sleeping for several milliseconds, to attempt to mitigate cloudflare 524 errors

        if (json_batch_iterator == json_batch_limit): # Handle batch files instead of 1 large file
            json_batch_iterator = 0
            write_json_to_disk(json_file_number, imdb_json_data) # Write the current json data from memory to disk
            json_file_number += json_batch_limit # Next time the filename will be iterated
            imdb_json_data = [] #Emptying past batch JSON data from memory

        try:
            current_target = omdb.request(i=current_tag, r='json', plot='full', apikey=api_key, timeout=5) #This is where we request data from OMDBAPI.com
        except:
            skipped_ids.append(current_tag) #We want to keep track of the IDs which were skipped due to timeout errors!
            continue

        if(current_target.status_code != 200): #Check if the scraped data contains an error (such as exceeding the quantity of their database's contents)
            skipped_ids.append(current_tag) # We want to keep track of the IDs which were skipped due to timeout errors!
            continue
            # Perhaps rather than continuing, we could count the failed status codes. Would catch setting the range far greater than the quantity of omdbapi entries.
        else:
            try:
                imdb_json_data.append(current_target.json()) # Scrape succeeded. Store JSON.
                json_batch_iterator += 1 # Iterate the json batch number iterator
            except:
                skipped_ids.append(current_tag) # Write failure to disk!

        pbar.update(progress_iterator + 1) # Display incremented progress
        progress_iterator += 1 # Iterate the progress bar for next iteration

    write_json_to_disk(json_file_number, imdb_json_data) # Final output, likely not triggered the batch limit if statement trigger above
    write_failures_to_disk(skipped_ids)
    pbar.finish() #Once we've complete the scraping, end the progress bar.

if __name__ == '__main__':

    api_key = "PRIVATE_KEY" # Paid private key, don't publicly share nor change.
    #scrape_start = int(sys.argv[1])
    #scrape_stop = int(sys.argv[2]) # Range of imdb ids we will generate and scrape.
    json_batch_limit = 1000

    #check_json_existence = check_json_files((scrape_stop - scrape_start))

    #if (check_json_existence == None):
    #omdb_id_list_holder = generate_imdb_tt_list(scrape_start, scrape_stop)
    #print(omdb_id_list_holder[-1])

    imdb_ids = []

    with open('title.basics.tsv') as f:
            table = pandas.read_table(f)
            imdb_ids = (table[table['titleType'] == 'movie']['tconst']).tolist()
            imdb_ids = list(set(imdb_ids))
            print("Quantity movies: " + str(len(imdb_ids)))

    scrape_omdb(len(imdb_ids), imdb_ids, 0)
    #else:
    #    new_scrape_start = (check_json_existence * json_batch_limit)
    #    #print(check_json_existence)
    #    #print(new_scrape_start)
    #    omdb_id_list_holder = generate_imdb_tt_list(new_scrape_start, scrape_stop)
    #    scrape_omdb((scrape_stop - new_scrape_start), omdb_id_list_holder, check_json_existence)
