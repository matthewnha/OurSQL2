import threading


def with_protection(f):
    def wrapper(*args):
        lock_inst = args[0]
        with lock_inst.protection:
            return f(*args)

    return wrapper


class XLock:
    def __init__(self, resource):
        self.protection = threading.Lock()
        self.owner = None
        self.resource = resource

    @with_protection
    def acquire(self, block):
        '''
        User acquiring lock

        block: compatibility, ignored
        '''

        curr_thread = threading.currentThread()

        if curr_thread == self.owner:
            return True

        if self.owner != None or self.resource.get_shared_count() > 0:
            return False

        self.owner = curr_thread
        return True

    @with_protection
    def release(self):

        curr_thread = threading.currentThread()

        if self.owner != curr_thread:
            raise Exception("Trying to release non-existant x lock")

        self.owner = None

    @with_protection
    def is_locked(self):
        return self.owner != None


class SLock:
    def __init__(self, resource):
        self.protection = threading.Lock()
        self.owners = []
        self.resource = resource

    @with_protection
    def acquire(self, block):
        '''
        User acquiring lock

        block: compatibility, ignored
        '''

        curr_thread = threading.currentThread()

        if curr_thread in self.owners:
            return True

        if self.resource.is_x_locked():
            return False

        self.owners.append(curr_thread)
        return True

    @with_protection
    def release(self):

        curr_thread = threading.currentThread()

        try:
            idx = self.owners.index(curr_thread)
            del self.owners[idx]
        except ValueError:
            raise Exception("Trying to release non-existant s lock")

    @with_protection
    def get_count(self):
        return len(self.owners)


class Resource:
    def __init__(self):
        self.x_lock = XLock(self)
        self.s_lock = SLock(self)

    def is_x_locked(self):
        return self.x_lock.is_locked()

    def get_shared_count(self):
        return self.s_lock.get_count()

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
