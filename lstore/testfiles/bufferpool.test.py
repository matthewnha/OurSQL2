from db import Database
from query import Query
from time import process_time
from random import seed, choice, randrange, randint
seed(1234)

from mergejob import MergeJob
import threading
import time
import statistics
import sys

# Student Id and 4 grades


# file = open('/Users/matthewha/ECS165/Grades/pagerange_0', "r+b")
# content = file.read()
# print('Start stuff')


def test1():
    expected = {}
    keys = []

    db = Database()
    db.open('~/ECS165')
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    # Measuring Insert Performance
    insert_time_0 = process_time()
    for i in range(0, 100):
        data = [i, i*10, i*20, i*30, i*40]
        query.insert(*data)

        keys.append(i)
        expected[i] = data
    insert_time_1 = process_time()

    print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

    # Measuring update Performance

    update_time_0 = process_time()
    for i in range(0, 10):
        col = randint(0, 4)
        val = randint(2000, 3000)
        data = [None, None, None, None, None]
        data[col] = val

        key = choice(keys)
        query.update(key, *data)
        expected[key][col] = val


    db.close()
    del db
    del grades_table
    del query

    imported_db = Database()
    imported_db.open('~/ECS165')
    grades_table = imported_db.get_table('Grades')
    query = Query(grades_table)


    # Measuring Insert Performance
    insert_time_0 = process_time()
    for i in range(100, 200):
        data = [i, i*10, i*20, i*30, i*40]
        query.insert(*data)

        keys.append(i)
        expected[i] = data
    insert_time_1 = process_time()

    print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

    # Measuring update Performance

    update_time_0 = process_time()
    for i in range(0, 10):
        col = randint(0, 4)
        val = randint(2000, 3000)

        data = [None, None, None, None, None]
        data[col] = val

        key = choice(keys)
        query.update(key, *data)
        expected[key][col] = val


    imported_db.close()
    del imported_db
    del imported_grades_table
    del imported_query

    db = Database()
    db.open('~/ECS165')
    grades_table = db.get_tables("Grades")
    query = Query(grades_table)

    success = 0
    for key in keys:
        result = query.select(key, [1,1,1,1,1])[0].columns
        if result != expected[key]:
            raise Exception("Didn't match ):")
        else:
            success += 1

    print(success,'/',len(keys),' were read successfully :)')

def test2():
    db = Database()
    db.open('~/ECS165')

    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    expected = {}
    keys = []


    print('START FIRST SELECT')

    insert_time_0 = process_time()
    for i in range(0, 10000):
        data = [i, i*10, i*20, i*30, i*40]
        query.insert(*data)

        keys.append(i)
        expected[i] = data
    insert_time_1 = process_time()

    for i in range(1000):

        # update
        col = randint(0, 4)
        val = randint(0, 100)

        data = [None, None, None, None, None]
        data[col] = val
        key = choice(keys)

        query.update(key, *data)
        expected[key][col] = val
        # print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected)))

        actual = query.select(key, [1,1,1,1,1])[0].columns
        # print('{0:>20} : {1:<10}'.format('SEL MERGE', str(actual)))
        if expected[key] != actual:
            raise Exception('shit')

    db.close()

    db = Database()
    db.open('~/ECS165')

    grades_table = db.get_table("Grades")
    query = Query(grades_table)

    print('START FIRST SELECT')

    insert_time_0 = process_time()
    for i in range(10000, 20000):
        data = [i, i*10, i*20, i*30, i*40]
        query.insert(*data)

        keys.append(i)
        expected[i] = data
    insert_time_1 = process_time()

    for i in range(1000):
        # update
        col = randint(0, 4)
        val = randint(0, 100)

        data = [None, None, None, None, None]
        data[col] = val
        key = choice(keys)

        query.update(key, *data)
        expected[key][col] = val
        # print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected)))

        actual = query.select(key, [1,1,1,1,1])[0].columns
        # print('{0:>20} : {1:<10}'.format('SEL MERGE', str(actual)))
        if expected[key] != actual:
            raise Exception('shit')

    db.close()

    # [1, 100, 88, 7, 200]

test2()