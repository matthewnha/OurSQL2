from page import Page
# from __main__config import *
from table import Table
from query import Query
from db import Database
from random import choice, randint, sample, seed

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

students = dict()

def create_new_student(name, student_id, *grades):
  students[name] = student_id
  return query.insert(student_id, *(grades[0]))

def update_student(student_id, grade, assignment):
  update = [None for i in range(5)]
  update[assignment] = grade
  return query.update(student_id, *update)

def sum_grades(id_start, id_end, assignment):
  return query.sum(id_start, id_end, assignment)

def delete_student(id):
  return query.delete(id)

def get_student_grades(key, q):
  return query.select(key, [0,1,1,1,1])


def handle_help():
  print('Commands:')
  print('=================================')
  print('help: Show commands')
  print('new: Create new user and his grades')
  print('')

def handle_new():
  name = input("Student name: ")
  id = int(input("Student ID: "))

  grades = []

  while len(grades) != 4:
    grades = input("Grades (comma-separated): ") # type: str
    grades = [int(str) for str in grades.split(',')]

    if len(grades) != 4:
      print("Error: please enter 4 grades...")

  if len(grades) != 4:
    return False

  ok = create_new_student(name, id, grades)

  return ok
  
def handle_grades():
  id = int(input("Student ID: "))

  grades = []

  while len(grades) != 4:
    grades = input("Grades to fetch (comma-separated bits): ") # type: str
    grades = [int(str) for str in grades.split(',')]

    if len(grades) != 4:
      print("Error: please enter 4 grades...")

  if len(grades) != 4:
    return False

  query = [0] + grades
  fetched = get_student_grades(id, query)
  print(fetched[0].columns)

  return True

def handle_assignment():
  pass

def handle_delete():
  pass

def handle_sum():
  pass

switcher = {
  'help': handle_help,
  'new': handle_new, # New user
  'grades': handle_grades, # Get grades
  'assignment': handle_assignment, # Update 1 assignment
  'delete': handle_delete, # Delete student
  'sum': handle_sum # Sum grades
}

def main():
  last_page = -1
  last_command = None

  while True:
    command = input("Enter command: ") 
    last_command = command

    
    if command == 'stop':
      break
    print("")  
    switcher[command]()
    print("")
    
main()