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

    db = Database()
    db.open('~/ECS165')
    grades_table = db.tables["Grades"]
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


    db.close()

    db = Database()
    db.open('~/ECS165')
    grades_table = db.tables["Grades"]
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
        print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected[key])))

        actual = query.select(key, [1,1,1,1,1])[0].columns
        print('{0:>20} : {1:<10}'.format('SEL MERGE', str(actual)))
        if expected[key] != actual:
            raise Exception('shit')

    db.close()

    print('Closed first time, opening')

    db = Database()
    db.open('~/ECS165')

    grades_table = db.tables["Grades"]
    query = Query(grades_table)

    print('START SECOND SELECT')

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
        print('{0:>20} : {1:<10}'.format('EXPECTED', str(expected[key])))

        actual = query.select(key, [1,1,1,1,1])[0].columns
        print('{0:>20} : {1:<10}'.format('SEL MERGE', str(actual)))
        if expected[key] != actual:
            raise Exception('shit')

    db.close()

    # [1, 100, 88, 7, 200]

test2()

# select_times_before = []
# agg_times_before = []

# for _ in range(3):
#     print('')
#     # Measuring Select Performance
#     select_time_0 = process_time()
#     for i in range(0, 10000):
#         query.select(choice(keys), [1, 1, 1, 1, 1])
#     select_time_1 = process_time()
#     elapsed = select_time_1 - select_time_0
#     select_times_before.append(elapsed)
#     print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

#     # Measuring Aggregate Performance
#     agg_time_0 = process_time()
#     for i in range(0, 10000, 100):
#         result = query.sum(906659671+i, 906659671+100, randrange(0, 5))
#     agg_time_1 = process_time()
#     elapsed = agg_time_1 - agg_time_0
#     agg_times_before.append(elapsed)
#     print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)
#     print('')

# select_times_after = []
# agg_times_after = []

# for _ in range(3):
#     print('')
#     # Measuring Select Performance
#     select_time_0 = process_time()
#     for i in range(0, 10000):
#         query.select(choice(keys), [1, 1, 1, 1, 1])
#     select_time_1 = process_time()
#     elapsed = select_time_1 - select_time_0
#     select_times_after.append(elapsed)
#     print("Selecting 10k records took:  \t\t\t", elapsed)

#     # Measuring Aggregate Performance
#     agg_time_0 = process_time()
#     for i in range(0, 10000, 100):
#         result = query.sum(906659671+i, 906659671+100, randrange(0, 5))
#     agg_time_1 = process_time()
#     elapsed = agg_time_1 - agg_time_0
#     agg_times_after.append(elapsed)
#     print("Aggregate 10k of 100 record batch took:\t", elapsed)
#     print('')

# print('Mean select before', statistics.mean(select_times_before))
# print('Mean select after', statistics.mean(select_times_after))
# print('Mean aggr before', statistics.mean(agg_times_before))
# print('Mean aggr after', statistics.mean(agg_times_after))

# # Measuring Delete Performance
# delete_time_0 = process_time()
# for i in range(0, 10000):
#     query.delete(906659671 + i)
# delete_time_1 = process_time()
# print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

# stop = True