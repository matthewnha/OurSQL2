from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed

db = Database()

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

inserted = []
for i in range(10000):
    cols = [947032+i, randint(0,20), randint(0,20), randint(0,20), randint(0,20)]
    query.insert(*cols)
    inserted.append(cols)

for row in inserted:
    fetched = query.select(row[0], [1,1,1,1,1])[0]['columns']
if fetched != row:
    raise Exception('Not match', fetched, row)
else:
    print('Matched', fetched, row)


query.insert(9399395, 5, 6, 7, 8)
query.update(9399395, *([None, 99, 88, None, None]))
query.update(9399395, *([None, 100, None, None, None]))
query.update(9399395, *([None, None, None, None, 200]))
query.update(9399395, *([1, None, None, None, None]))
print(query.select(9399395,[1,1,1,1,1]))
query.delete(9399395)
#print(query.select(9399395,[1,1,1,1,1]))
print(query.select(947050,[0,0,1,1,1]))
print(query.select(947111,[0,0,1,1,1]))
print(query.sum(947043,947111,3))
# print(query.select(9399395, [1,1,1,1,1])[0])


