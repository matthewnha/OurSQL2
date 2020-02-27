from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed

db = Database()
db.open("BP_Test")

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

inserted = []
for i in range(10):
    cols = [100+i, 1+i, 1+i, 1+i, 1+i]
    query.insert(*cols)
    inserted.append(cols)

# for row in inserted:
#     print(row)
#     fetched = query.select(row[0], [1,1,1,1,1])[0].columns
#     if fetched != row:
#         raise Exception('Not match', fetched, row)
#     else:
#         print('Matched', fetched, row)

for i,row in enumerate(inserted):
    # if i%2 == 0:
        update_col = [None for a in range(5)]
        update_col[i%4+1] = i*5
        query.update(100+i,*update_col)
        inserted[i][i%4+1] = i*5

for row in inserted:
    fetched = query.select(row[0], [1,1,1,1,1])[0].columns
    if fetched != row:
        raise Exception('Not match', fetched, row)
    else:
        print('Matched', fetched, row)

db.close()
del db
del grades_table
del query

imported_db = Database()
imported_db.open("BP_Test")
grades_table = imported_db.get_table('Grades')
query = Query(grades_table)


for row in inserted:
    fetched = query.select(row[0], [1,1,1,1,1])[0].columns
    if fetched != row:
        raise Exception('Not match', fetched, row)
    else:
        print('Matched', fetched, row)
