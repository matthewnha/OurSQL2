from config import *

def parse_schema_enc_from_bytes(bytes):
  '''
    Takes bytes of schema encoding and converts it to a string 
  '''
  ret_enc = ''
  for byte in bytes:
      ret_enc += '' + str(byte)
  return ret_enc

def int_from_bytes(bytes):
  return int.from_bytes(bytes, ByteOrder)

def int_to_bytes(data):
  return data.to_bytes(CellSizeInBytes, ByteOrder)
