# wget https://datasets.imdbws.com/title.basics.tsv.gz

import pandas

with open('title.basics.tsv') as f:
        table = pandas.read_table(f)
        imdb_ids = (table[table['titleType'] == 'movie']['tconst']).tolist()
        imdb_ids = list(set(imdb_ids))
        print("Quantity movies: " + str(len(imdb_ids)))

with open("imdb_ids.csv", "w") as csv_file:
    check = 0
    for id in imdb_ids:
        if (check == 0):
            csv.file.write('id' + '\n')
        else:
            csv_file.write(id + '\n')
        check += 1
