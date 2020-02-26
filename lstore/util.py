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

def ifb(_bytes):
    '''
    Int from bytes
    '''
    return int_from_bytes(_bytes)


def int_to_bytes(data):
    return data.to_bytes(CELL_SIZE_BYTES, BYTE_ORDER)


def itb(data):
    '''
    Int to bytes
    '''
    return int_to_bytes(data)


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
        mult = (outer_index - base_index) / (container_size)  # type: float

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


def col_encoding_to_binary(list, truthy=1, falsey=0):
    bin = 0
    for i, bit in enumerate(list):
        if bit == truthy or bit != falsey:
            bin += 2**i

    return bin

def cetb(list, truthy=1, falsey=0):
  '''
  Column encoding to binary number
  
  cetb((OBJ, None, OBJ, None), falsey=None)
  '''
  return col_encoding_to_binary(list, truthy, falsey)


def check_col_encoding(bin_enc, col):
    '''
    [0,1,2,3]
    '''
    mask = 2**col
    return mask & bin_enc == mask


def encode_pid(pid):  # 24 bytes
    out = b''
    for idx in pid:
        out += int_to_bytes(idx)

    return out


def decode_pid(bytes_pid):  # 24 bytes
    inner_idx = int_from_bytes(bytes_pid[0:8])
    page_idx = int_from_bytes(bytes_pid[8:16])
    pr_idx = int_from_bytes(bytes_pid[16:24])

    return (inner_idx, page_idx, pr_idx)


def test():
    enc = [(1, 0, 0), None, (1, 0, 0), None]
    bin = col_encoding_to_binary(enc, falsey=None)
    by = int_to_bytes(bin)
    read_enc = int_from_bytes(by)
    print([check_col_encoding(read_enc, i) for i in range(4)])

    pid = (123, 456, 789)
    print(decode_pid(encode_pid(pid)))


if __name__ == '__main__':
    test()
