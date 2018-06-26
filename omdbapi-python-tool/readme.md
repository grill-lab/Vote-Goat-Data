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

## Take note of any failures

There may be failures due to connection issues or JSON parsing issues, take note of them and try to scrape individually.
