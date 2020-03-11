import threading

def with_protection(f):
    def wrapper(*args):
        lock_inst = args[0]
        with lock_inst.protection:
            return f(*args)

    return wrapper

class XLock:
  def __init__(self, parent):
    self.protection = threading.Lock()
    self.owner = None
    self.parent = parent

  @with_protection
  def acquire(self): 
    '''
    User acquiring lock
    '''

    curr_thread = threading.currentThread()

    if curr_thread == self.owner:
      return True

    if self.owner != None or len(self.parent.get_shared()) > 0:
      return False
    
    self.owner = curr_thread
    return True

  @with_protection
  def release(self):

    curr_thread = threading.currentThread()

    if self.owner != curr_thread:
      raise Exception("Trying to release non-existant x lock")

    self.owner = None

class SLock:
    def __init__(self, parent):
      self.protection = threading.Lock()
      self.owners = []
      self.parent = parent

    @with_protection
    def acquire_s_lock(self): 
      '''
      User acquiring lock
      '''

      curr_thread = threading.currentThread()
      
      if curr_thread in self.shared:
        return True

      if self.parent.get_exclusive() != None:
        return False
      
      self.ownered.append(curr_thread)
      return True

class Lock:
  def __init__(self, id):
    self.id = id

    self.protection = threading.Lock()

    self.shared = []
    self.exclusive = None

  @with_protection
  def acquire_x_lock(self, lockee): 
    '''
    User acquiring lock
    '''

    lockee = threading.currentThread()

    if lockee == self.exclusive:
      return True

    if self.exclusive != None or len(self.shared) > 0:
      return False
    
    self.exclusive = lockee
    return True

  @with_protection
  def acquire_s_lock(self, lockee): 
    '''
    User acquiring lock
    '''

    lockee = threading.currentThread()
    
    if lockee in self.shared:
      return True

    if self.exclusive != None:
      return False
    
    self.shared.append(lockee)
    return True

  @with_protection
  def release_x_lock(self, lockee):

    lockee = threading.currentThread()

    if self.exclusive != lockee:
      raise Exception("Trying to release non-existant x lock")

    self.exclusive = None

  @with_protection
  def release_s_lock(self, lockee):

    lockee = threading.currentThread()

    try:
      idx = self.shared.index(lockee)
      del self.shared[idx]
    except ValueError:
      raise Exception("Trying to release non-existant s lock")

class LockManager:
  def __init__(self):
    self.locks = {}

def get_lock(self, id):
  if id not in self.locks:
    self.locks[id] = Lock(id)
  return self.locks[id]

def acquire_all(locks):
  acquired = []

  for lock in locks:
      is_acquired = lock.acquire(False)

      if not is_acquired:
          for to_release in acquired:
              to_release.release()

          # print('Couldn\'t acquire all locks')
          return False

      acquired.insert(0, lock)

  return acquired


def release_all(locks):
  for lock in locks:
      lock.release()