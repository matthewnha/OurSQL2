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

inserted = {}
for i in range(1000):
    cols = [100+i, 1+i, 1+i, 1+i, 1+i]
    query.insert(*cols)
    inserted[100+i] = cols

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
        a = 0
        # print('Matched', fetched, row)

keys = sorted(list(inserted.keys()))
deleted_keys = sample(inserted, 100)

for record in deleted_keys:
    query.delete(record)
    inserted.pop(i, None)

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
        # print('Matched', fetched, row)
        a = 0

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
        a = 0
        # print('Matched', fetched, row)
     
query.insert(*[2000,666,666,666,666])
print(query.select(200,[1,1,1,1,1])[0].columns)
query.update(105,*[None,None,None,100,None])
print(query.select(105,[1, 1, 1, 1, 1])[0].columns)
query.update(200,*[None,555,None,None,555])
print(query.select(200,[1,1,1,1,1])[0].columns)
query.update(200,*[1,None,55,None,None])
print(query.select(200,[1,1,1,1,1])[0].columns)
imported_db.close()
