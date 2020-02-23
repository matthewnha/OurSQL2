with open('./test.data', 'w+b') as file:
  file.seek(0)
  val = 2**64-1
  file.write(val.to_bytes(8, 'little'))

  file.seek(8)
  file.write(bytes('AAAAAAAA', 'utf-8'))

  file.seek(0)
  read = file.read(8)
  print(read)

  file.seek(8)
  read = file.read(8)
  print(read)