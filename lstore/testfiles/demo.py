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
  students[student_id] = name
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
  return query.select(key, q)


def handle_help():
  print('Commands:')
  print('==')
  print('help: Show commands')
  print('new: Create new student and his grades')
  print('grades: Print student\'s grades')
  print('assignment: Update student\'s grade on an assignment')
  print('delete: Delete a student\'s records')
  print('sum: Get the sum of grades for an assignment')
  print('stop: Stop program')

  return True

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

  try:
    ok = create_new_student(name, id, grades)
  except:
    print("Not Valid")
    return False

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
  try:
    fetched = get_student_grades(id, query)[0].columns
  except Exception:
    print("Student not found.")
    return False

  for i, q in enumerate(grades):
    if q == 1:
      print('Assignment ' + str(i) + ':', fetched[i+1])

  return True

def handle_assignment():
    id = int(input("Student ID: "))

    grade = None # type: str
    
    while type(grade) is not int:

        try :
            grade = input("New Grade: ") 
            grade = int(grade)
        except ValueError:
            print("Not a valid grade")

    assign = None
    while type(assign) is not int:
        assign = input ("Which grade to change (int from 1 to 4)? :")

        try:
            assign = int(assign)
        except ValueError:
            print("Not a valid number.")

        if assign > 4 or assign < 1:
            print("Not in range.")
            assign = None

    try: 
        updated = update_student(id,grade,assign)
    except:
        print("Not a valid key.")
        return False
    return updated

def handle_delete():

    id = None 
    while type(id) is not int:
      try:
        deleteKey = input("Please enter Student ID to delete: ")
        id = int(deleteKey)
      except ValueError:
        print("Not a number")

    try:
      query.delete(id)
    except:
      print("Not a student.")
      return False

    del students[id]
    print(deleteKey + " id deleted.")
    return True

def handle_sum():
  print("~ Summing up grades ~")
  start_sid = int(input("Enter start SID: "))
  end_sid = int(input("Enter end SID: "))

  assignment = 0
  while 0 == assignment:
    assignment = int(input("Assignment to sum up: "))
    if assignment < 1 or assignment > 4:
      print('Error: Please enter an assignment from 1-4')
      assignment = 0

  sum = sum_grades(start_sid, end_sid, assignment)
  print('\n', "Sum of assignment " + str(assignment) + ":", sum)

  return True

def handle_students():
  print("{:<20}{:<15}".format("Student", "ID"))
  print("{:<20}{:<15}".format("------", "------"))

  for id in sorted(students):
    name = students[id]
    row = "{:<20}{:<15}".format(name, id)
    print(row)

  return True

switcher = {
  'help': handle_help,
  'new': handle_new, # New user
  'grades': handle_grades, # Get grades
  'assignment': handle_assignment, # Update 1 assignment
  'delete': handle_delete, # Delete student
  'sum': handle_sum, # Sum grades
  'students' : handle_students # Gives student list
}

def main():
  last_page = -1
  last_command = None

  while True:
    command = input("Enter command: ") 
    last_command = command.strip()

    if command == 'stop':
      break
    print("\n=============================\n")  
    try:
      func = switcher[command.strip()]
    except:
      print("Command not valid. Type 'help' for list of commands. \n")
      continue
    ok = switcher[command.strip()]()

    if ok:
      print("\nComplete!")
    else:
      print("\nFailed :(")
    
    print("=============================\n")

main()