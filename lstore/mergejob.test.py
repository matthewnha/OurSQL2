from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed
from mergejob import MergeJob

db = Database()

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

query.insert(9399395, 5, 6, 7, 8)
query.update(9399395, *([None, 99, 88, None, None]))
query.update(9399395, *([None, 100, None, None, None]))
query.update(9399395, *([None, None, None, None, 200]))
query.update(9399395, *([1, None, None, None, None]))

job = MergeJob(grades_table)
job.run()