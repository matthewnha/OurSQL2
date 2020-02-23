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

        end = time.mktime(time.localtime())
        # print('Merge done in', time.strftime("%X", time.localtime(end-start)))
        time.sleep(1)

scheduler = threading.Thread(target=schedule_merge, args=())
scheduler.start()

query.insert(9399395, 5, 6, 7, 8)
query.update(9399395, *([None, 99, 88, None, None]))
query.update(9399395, *([None, 100, None, None, None]))
query.update(9399395, *([None, None, None, None, 200]))
query.update(9399395, *([1, None, None, None, None]))

scheduler.join()