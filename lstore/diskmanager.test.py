from db import Database
from diskmanager import DiskManager
from query import Query
from config import *

from random import choice, randint, sample, seed
from colorama import Fore, Back, Style
import filereader

# Student Id and 4 grades   
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

seed(3562901)

for i in range(0, 10):
    key = 92106429 + randint(0, 90)
    while key in records:
        key = 92106429 + randint(0, 900)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])

for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    # if error:
    #     print('select error on', key , ':', record, ', correct:', records[key])
    # else:
    #     print('select on', key, ':', record.columns)

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        # if error:
        #     print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key].columns)
        # else:
        #     print('update on', original, 'and', updated_columns, ':', record.columns) 
        # updated_columns[i] = None
db.my_manager.write_db_directory()

for key in db.tables.keys():
    print(key, "Table")
    db.my_manager.write_table_meta(key)
    table = db.tables[key]


filereader.read_files()