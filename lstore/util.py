from config import *

def parse_schema_enc_from_bytes(enc_bytes):
  '''
    Takes bytes of schema encoding and converts it to a string 
  '''

  ret_enc = ''
  for byte in enc_bytes:
      ret_enc += '' + str(byte)
  return ret_enc

def int_from_bytes(from_bytes):
  return int.from_bytes(from_bytes, BYTE_ORDER)

def int_to_bytes(data):
  return data.to_bytes(CELL_SIZE_BYTES, BYTE_ORDER)

def get_inner_index_from_outer_index(outer_index, container_size):
  '''
    List 1
    | 0 1 2 | | 3 4 5 | | 6 7 8 |
    0 -> 0, 3 -> 0, 6 -> 0
    1 -> 1, 4 -> 1, 7 -> 1
    2 -> 2, 5 -> 2, 8 -> 2,
  '''

  if outer_index < container_size:
    return outer_index

  for i in range(container_size):
    base_index = i
    mult = (outer_index - base_index) / (container_size) # type: float

    if mult.is_integer():
      break

  return base_index

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