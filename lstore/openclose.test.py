from db import Database
from query import Query
from time import process_time
from random import choice, randrange

from mergejob import MergeJob
import threading
import time
import statistics

# Student Id and 4 grades

# file = open('/Users/matthewha/ECS165/Grades/pagerange_0', "r+b")
# content = file.read()
# print('Start stuff')

db = Database()
db.open('~/ECS165')


imported = False
try:
  grades_table = db.tables["Grades"]
  imported = True
except:
  grades_table = db.create_table('Grades', 5, 0)
# Student Id and 4 grades
query = Query(grades_table)
keys = []

# db.my_manager.rw_test()
print('Done opening database')

extra = 0
if imported:
  extra = 10000

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i + extra, 93, 0, 0, 0)
    keys.append(906659671 + i + extra)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

db.close()