from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed

db = Database()

grades_table = db.create_table('Grades', 0, 5)
query = Query(grades_table)

# inserted = []
# for i in range(300):
#   cols = [947032+i, randint(0,20), randint(0,20), randint(0,20), randint(0,20)]
#   query.insert(*cols)
#   inserted.append(cols)

# for row in inserted:
#   fetched = query.select(row[0], [1,1,1,1,1])
#   if fetched != row:
#     raise Exception('Not match', fetched, row)
#   else:
#     print('Matched', fetched, row)

query.insert(9399394, 1, 2, 3, 4)
query.update(9399394, [None, None, 6, None, None])
# query.update(9399394, [None, 10, None, None, None])
# query.update(9399394, [None, 11, None, None, 9])

query.insert(9399395, 5, 6, 7, 8)
# query.update(9399395, [None, 99, 88, None, None])
# query.update(9399395, [None, 100, None, None, None])
# query.update(9399395, [None, None, None, None, 200])

print(query.select(9399394, [1,1,1,1,1])[0])
# print(query.select(9399395, [1,1,1,1,1])[0])

# query.delete(9399394)

# print('shouldnnt', query.select(9399394, [1,1,1,1,1])[0])

print(query.sum(9399394, 9399395, [1, 1, 1, 1, 1]))