import logging
import threading
import time

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

val = 0
lock = threading.Lock()
locks = {}

def thread_a():
  with lock:
    print('A got that fucking lock on dat shit')
    val = 1
    time.sleep(5)

  print('A released lock')

def thread_b():
  with lock:
    print('B got that fucking lock on dat shit')
    val = 2
    time.sleep(1)
  
  print('B released thread')

  
    

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    # threads = list()
    # for index in range(3):
    #     logging.info("Main    : create and start thread %d.", index)
    #     x = threading.Thread(target=thread_function, args=(index, ))
    #     threads.append(x)
    #     x.start()

    # for index, thread in enumerate(threads):
    #     logging.info("Main    : before joining thread %d.", index)
    #     thread.join()
    #     logging.info("Main    : thread %d done", index)

    a = threading.Thread(target=thread_a, args=())
    b = threading.Thread(target=thread_b, args=())
    print('Main thread')


    a.start()
    b.start()

    with lock:
      print('Main got that fucking lock on dat shit')
      val = 3
      time.sleep(1)
    
    print('Main released thread')
