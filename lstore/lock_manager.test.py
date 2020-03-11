from lock_manager import Lock
import threading
from time import sleep

lock = Lock(0)

class SharedLock(threading.Thread):
    def __init__(self):
        super(SharedLock, self).__init__()

    def run(self):
      suc = lock.acquire_s_lock(self)
      if suc:
        print('Acquired shared lock')
      else:
        print("Couldn't acquire shared lock")

class LoopSharedLock(threading.Thread):
    def __init__(self):
        super(LoopSharedLock, self).__init__()

    def run(self):
      while(True):
        suc = lock.acquire_s_lock(3)
        if suc:
          print('Acquired shared lock')
        else:
          print("Couldn't acquire shared lock")

class Thread4(threading.Thread):
  def __init__(self):
    super(Thread4, self).__init__()

  def run(self):
    suc = lock.acquire_x_lock(4)
    if suc:
      print('Acquired x lock')
    else:
      print("Couldn't acquire x lock")

    sleep(3)
    
    lock.release_x_lock(4)

class Thread5(threading.Thread):
  def __init__(self):
    super(Thread5, self).__init__()

  def run(self):
    for _ in range(10):
      suc = lock.acquire_x_lock(5)
      if suc:
        print('Acquired x lock')
        break
      else:
        print("Couldn't acquire x lock")
      sleep(1)
  



threads = [
  Thread4(),
  SharedLock(),
  SharedLock(),
  # LoopSharedLock(),
  Thread5(),
]

for thread in threads:
  thread.start()

for thread in threads:
  thread.join()