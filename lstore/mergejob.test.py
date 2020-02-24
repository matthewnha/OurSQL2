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

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

expected = [9399395, 5, 6, 7, 8]
stop = False

def merge_thread():
    job = MergeJob(grades_table)
    job.run()

def schedule_merge():
    while(1):
        # print('Merge starting.')
        start = time.mktime(time.localtime())

        merge = threading.Thread(target=merge_thread, args=())

        merge.start()
        merge.join()
        print('Merged')

        end = time.mktime(time.localtime())
        # print('Merge done in', time.strftime("%X", time.localtime(end-start)))
        time.sleep(1)

        if stop:
            return

scheduler = threading.Thread(target=schedule_merge, args=())
scheduler.start()

print('START FIRST SELECT')
query.insert(9399395, 5, 6, 7, 8)
for i in range(100):

    # select
    start = time.mktime(time.localtime())
    actual = query.select(9399395, [1,1,1,1,1])[0].columns
    end = time.mktime(time.localtime())
    print(end-start)
    print('{0:>20} : {1:<10} in {2:<10}'.format('SEL MERGE', str(actual), str(end-start)))
    if expected != actual:
        raise Exception('shit')

    # update
    col = randint(0, 4)
    val = randint(0, 100)

    data = [None, None, None, None, None]
    data[col] = val

    query.update(9399395, *data)
    expected[col] = val
    print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected)))

    time.sleep(0.1)

stop = True
print('END FIRST SELECT')

# [1, 100, 88, 7, 200]