# OMDBAPI scraping tool!

This tool scrapes all movies from OMDBAPI & saves to JSON files.

You can run this from the python command line & it will show a progress bar then output the JSON to the 'data.json' file.

Prior to running this tool, change the range value.

# Dependencies

omdb
time (default)
math (default)
ujson
multiprocessing
pandas

## Get target movie ids

The downloaded file includes all IMDB content, we filter all entries except those which have type 'movie', if you want to change this then edit the ```produce_imdb_id_list.py``` script 👍.

```
wget https://datasets.imdbws.com/title.basics.tsv.gz
gunzip title.basics.tsv.gz
python3 produce_imdb_id_list.py
```

## Configure & run dump script

Configure the worker count in the `multiprocessed_omdb_dump.py` file, each worker consumes 1-5% CPU, the more workers the faster the dump.

```
python3 multiprocessed_omdb_dump.py
```

NOTE: Use 'screen' for a persistent terminal session, preventing the script from being killed before completion.

Expected output: (parse failures require additional investigation)
```
Quantity movies: 489166
Failed to parse: tt4601148
Failed to parse: tt2091423
Failed to parse: tt3517110
Failed to parse: tt0120690
Failed to parse: tt3335398
Failed to parse: tt3110260
Failed to parse: tt2365042
Failed to parse: tt4176748
Failed to connect: tt0271827
Failed to connect: tt0479253
Failed to parse: tt5497752
Failed to parse: tt4137568
Failed to connect: tt0353838
Failed to connect: tt0391728
Failed to parse: tt5791288
Failed to connect: tt0293631
Failed to parse: tt4426108
Retrieved movies: 489166
Time taken: 1536.7215342521667
```

## Process dumped data

Configure the ```insert_movies.py``` script to target your MongoDB database.

Inspect the ```reduce_movie_json.py``` script - you may wish to alter what movies you filter, such as adult or violent movies.

```
python3 reduce_movie_json.py
python3 insert_movies.py
```

## Take note of any failures

There may be failures due to connection issues or JSON parsing issues, take note of them and try to scrape individually.

```
python3 quantity_failures.py
# If the above reports failures, run the following.
python3 produce_failed_imdb_id_list.py
# Move the CSV produced by the above in place of the main imdb id target csv file
python3 multiprocessed_omdb_dump.py
```

NOTE: [Approx 60.5k movies were not retrievable from OMDBAPI](https://github.com/omdbapi/OMDb-API/issues/74#issuecomment-400363435). Hopefully these will be fixed in the future.
