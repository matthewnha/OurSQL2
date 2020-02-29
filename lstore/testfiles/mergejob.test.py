from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed
from mergejob import MergeJob
import threading
import time

db = Database()
db.open('~/ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

expected = [9399395, 5, 6, 7, 8]
stop = False

def merge_thread():
    job = MergeJob(grades_table)
    job.run()

def schedule_merge():
    while(1):
        print('Merge starting.')
        start = time.mktime(time.localtime())

        merge = threading.Thread(target=merge_thread, args=())

        merge.start()
        merge.join()
        # print('Merged')

        end = time.mktime(time.localtime())
        print('Merge done in', time.strftime("%X", time.localtime(end-start)))
        time.sleep(5)

        if stop:
            return

scheduler = threading.Thread(target=schedule_merge, args=())
scheduler.start()

success = 0
count = 0

expected = {}
keys = []


print('START FIRST SELECT')

insert_time_0 = time.process_time()
for i in range(0, 10000):
    data = [i, i*10, i*20, i*30, i*40]
    query.insert(*data)

    keys.append(i)
    expected[i] = data
insert_time_1 = time.process_time()

for i in range(10000):

    # update
    col = randint(1, 4)
    val = randint(0, 100)

    data = [None, None, None, None, None]
    data[col] = val
    key = choice(keys)

    query.update(key, *data)
    expected[key][col] = val
    # print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected)))

    # if i % 2000 == 0:
    #     job = MergeJob(grades_table)
    #     job.run()

    actual = query.select(key, [1,1,1,1,1])[0].columns
    # print('{0:>20} : {1:<10}'.format('SEL MERGE', str(actual)))

    count += 1
    exp = expected[key]
    if exp != actual:
        raise Exception('shit')
    else:
        success += 1

db.close()


stop = True
print('END FIRST SELECT')

# [1, 100, 88, 7, 200]