import omdb # For scraping omdbapi.com
import ujson # For outputting to disk
from time import sleep # For sleeping between scrape attempts
import numpy as np # Not utilised past the failure text file function
import os # For checking the existing data files
import math # For rounding float up to nearest integer

# multiprocessing packages:
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from progressbar import Bar, ETA, ProgressBar, Percentage # For providing progressbar functionality. This is actually "progressbar33" in python3.

def del_file(file_number):
    """
    Delete a json file. Perhaps a def is overboard.
    """
    latest_file_name = 'data_' + (str(file_number).zfill(6)) + '.json'
    os.remove(latest_file_name)

#def check_json_files(range_ref):
#    """
#    Check for the existence of existing data_xxxxxx.json files.
#    If they do exist, delete the final data file (in case it is partially filled, or broken).
#    Return final data file - 1.
#    Purpose: Recover last known scraping state, avoiding manually finding the latest scrape_start value to tweak.
#    """
#    no_file = None # Perhaps we could just return 'None' instead of declaring ahead of the if statement?
#
#    if(os.path.exists('data_000000.json')):
#        latest_json_file = 0 # Init the default value
#
#        ceiling_range = int(math.ceil(range_ref/json_batch_limit)) + 1 # Could be float, so round up to integer!
#        #print("ceiling_range: {}".format(ceiling_range))
#        for i in range(ceiling_range):
#            #print("i: {}".format(i))
#            json_file_name = 'data_' + (str(i).zfill(6)) + '.json'
#            if(os.path.exists(json_file_name)):
#                #print("continue: {}".format(i))
#                latest_json_file = i
#                continue
#            else:
#                #print("break: {}".format(i))
#                break # We found the latest!
#        #print("deleting: {}".format(latest_json_file))
#        #print("delete: {}".format(i))
#        del_file(latest_json_file) # Delete the final file to avoid issues
#        #return (latest_json_file - 1) # Return the last remaining file number (after we deleted the latest file)
#    else:
#        return no_file

def generate_imdb_tt_list(start, stop):
    """
    Generate the list of IMDB id tags, which we will use to query omdbapi.
    This creates a list of lists, not a single list!
    Format: "tt + (fill: up to 6 zeros) number"
    """
    imdb_tag_list = [] # Empty list to hold our imdb tags
    list_chunk_size = int(math.ceil((stop - start)/json_batch_limit) + 1)
    external_iterator = 0

    for i in range(int(math.ceil((stop - start) / list_chunk_size)) + 1):

        internal_list = []
        for current_iteration in range(list_chunk_size):
            prepend_imdb_tag = 'tt' #Required prepended tag
            internal_list.append(prepend_imdb_tag+(str(external_iterator + 1).zfill(7))) #appending the number to tt & padding with zeros.
            external_iterator += 1

        imdb_tag_list.append(internal_list)

    return(imdb_tag_list) # Return the id list

def write_json_to_disk(file_name, json_data):
    """
    When called, write the json_data to a json file.
    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    with open(file_name, 'w') as outfile:
        ujson.dump(json_data, outfile, encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=False) #Write JSON to data.json (disk)

def write_failures_to_disk():
    """
    When called will write all timeout'd query IMDB ids.
    Future functionality could include these files
    """
    if (np.size(skipped_ids) > 0):
        with open("failure.txt", "w") as text_file:
            for skipped_item in skipped_ids:
                text_file.write(skipped_item + '\n')

def scrape_omdb(omdb_id_list):
    """
    When called, will scrape omdbapi for the range of imdb ids we generated during the generate_imdb_tt_list() step.
    Will display a progress bar so you know the script is still functioning.
    WARNING: If you set the range to 1 million records, expect the script to take up to 2-3 days.
    """

    imdb_json_data = {} #Empty data field
    imdb_json_data['items'] = [] #Empty data list titled 'items'
    progress_iterator = 0 #For the progress bar
    json_batch_iterator = 0 #Every json_batch_iterator iterations we will revert to 0, limiting the size of each json file.
    current_filename = str(omdb_id_list[0]) + "_" + str(omdb_id_list[-1]) + '.json' #Create a new fileformat

    for current_tag in omdb_id_list: #for loop iterate over the list of imdb tags we generated
        sleep(0.1) #Sleeping 100 milliseconds, to attempt to mitigate cloudflare 524 errors

        try:
            current_target = omdb.request(i=current_tag, r='json', plot='full', apikey=api_key, timeout=10) #This is where we request data from OMDBAPI.com
        except:
            skipped_ids.append(current_tag) #We want to keep track of the IDs which were skipped due to timeout errors!
            continue
        if(current_target.status_code != 200): #Check if the scraped data contains an error (such as exceeding the quantity of their database's contents)
            skipped_ids.append(current_tag) # We want to keep track of the IDs which were skipped due to timeout errors!
            continue
        else:
            if (current_target.json()['Response'] == "True"): # If false: Something has gone wrong!
                try:
                    imdb_json_data['items'].append(current_target.json()) # Scrape succeeded. Store JSON.
                    pbar.update(progress_iterator + 1) # Display incremented progress
                    progress_iterator += 1 # Iterate the progress bar for next iteration
                    json_batch_iterator += 1 # Iterate the json batch number iterator
                except:
                    skipped_ids.append(current_tag) # Write failure to disk!
            else:
                skipped_ids.append(current_tag) # Write failure to disk!

    write_json_to_disk(current_filename, imdb_json_data['items']) # Final output, likely not triggered the batch limit if statement trigger above

if __name__ == '__main__':
    #check_json_existence = check_json_files((scrape_stop - scrape_start)) # Check if any files exist already

    #if (check_json_existence == None):
    #    ref_file_name = 0 # Start the file numbering from 0
    #else:
    #    scrape_start = (check_json_existence * json_batch_limit) # New scraping location
    #    ref_file_name = check_json_existence # Start the fi;e numbering from where we left off

    api_key = "PRIVATE_KEY" # Paid private key, don't publicly share nor change.
    scrape_start = 1
    scrape_stop = 200 # Range of imdb ids we will generate and scrape.
    json_batch_limit = 10
    quantity_workers = 5
    skipped_ids = [] # Initializing a list to keep track of scraping attempts which timed out.

    omdb_id_list_holder = generate_imdb_tt_list(scrape_start, scrape_stop) # Generate our list of lists

    widgets = [Percentage(), # Setting how we wan the progress bar to look
               ' ', Bar(),
               ' ', ETA()]

    pbar_range = scrape_stop - scrape_start
    pbar = ProgressBar(widgets=widgets, maxval=pbar_range).start() #Prepare the progress bar

    pool = ThreadPool(quantity_workers) # Let's drop into hyperthread space!
    pool.map(scrape_omdb, omdb_id_list_holder) # Deploy the pool workers
    pool.close()
    pool.join()

    pbar.finish() #Once we've complete the scraping, end the progress bar.
