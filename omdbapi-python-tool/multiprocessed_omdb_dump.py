import omdb # For scraping omdbapi.com
import ujson # For outputting to disk
from time import sleep # For sleeping between scrape attempts
import os # For checking the existing data files
import math # For rounding float up to nearest integer
import sys # Argument handling
from multiprocessing import Pool
import pandas # For tsv parsing
import time

def write_json_to_disk(filename, json_data):
    """
    When called, write the json_data to a json file.
    We will end up with many data_*.json files.
    These files will be merged using jq.
    """
    with open(filename, 'w') as outfile:
      ujson.dump(json_data, outfile,  encode_html_chars=True, escape_forward_slashes=False, ensure_ascii=False, indent=4)

def scrape_omdb_id(omdb_id):
    """
    Given a single OMDB movie id (IMDB), return JSON movie metadata (or a failure message).
    The worker pool runs many processes for this function given our target id list.
    """
    time.sleep(0.1) # Adjustable sleep
    try:
        current_target = omdb.request(i=omdb_id, r='json', plot='full', apikey=api_key, timeout=5) #This is where we request data from OMDBAPI.com
    except:
        # Failed to contact OMDB
        try:
            time.sleep(1) #Sleeping
            current_target = omdb.request(i=omdb_id, r='json', plot='full', apikey=api_key, timeout=10) # 2nd attempt w/ greater timeout
        except:
            # Failed to contact OMDB twice
            current_target = None

    if (current_target != None):
        # We succeeded in contacting OMDB.
        try:
            # Current target JSON is valid
            current_target = current_target.json()
            if current_target['Response'] == "False":
                current_target['imdb_id'] = str(omdb_id)
            return current_target
        except:
            # Failed to parse current target JSON. Return failure.
            print("Failed to parse: {}".format(omdb_id))
            return {"Response": "False", "Error_Message": "Parse Error", "imdb_id": str(omdb_id)}
    else:
        # We failed to contact OMDB, return a failure
        print("Failed to connect: {}".format(omdb_id))
        return {"Response": "False", "Error_Message": "Connection Error", "imdb_id": str(omdb_id)}

if __name__ == '__main__':
    api_key = "API_KEY" # Paid private key, don't publicly share nor change.

    df = pandas.read_csv('./imdb_ids.csv') # Run 'produce_imdb_id_list.py' to get this data
    imdb_ids = list(set(df['id'].tolist())) # Remove any duplicates
    print("Quantity movies: " + str(len(imdb_ids)))

    start_time = time.time()

    pool = Pool(processes=30) # 30 workers - approx 1-5% single core per worker
    pool_imdb_data = pool.map(scrape_omdb_id, imdb_ids) # Deploy the pool workers

    """
    Note: The next step can require ~8GB RAM. If you hit memory issues, try the following lines:
        split = len(pool_imdb_data)/2
        write_json_to_disk('imdb_movies_0.json', pool_imdb_data[:split])
        write_json_to_disk('imdb_movies_1.json', pool_imdb_data[split+1:])
    """
    write_json_to_disk('imdb_movies.json', pool_imdb_data) # Final output, likely not triggered the batch limit if statement trigger above

    done_time = time.time()
    elapsed = done_time - start_time
    print("Time taken: {}".format(elapsed))

    #test_count = 0
    #for a in pool_imdb_data:
    #    if a['Response'] == "False":
    #        test_count = test_count + 1
    #print("Retrieved movies: {}".format(str(len(imdb_ids) - test_count)))
