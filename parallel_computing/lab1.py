import random
from threading import Thread
from queue import Queue
import time


def fill_in_matrix(N):
    mat = [[random.randint(0, N**3) for _ in range(N)] for _ in range(N)]
    return mat


def worker(q, result_queue):
    while not q.empty():
        work = q.get()
        min = None
        for el in work[0]:
            if min is None:
                min = el
            elif el < min:
                min = el
        res = (min, work[1])
        result_queue.put(res)


def matrix(q, mat, N):
    while not q.empty():
        work = q.get()
        min_el = work[0]
        index = work[1]
        mat[N-index-1][index] = min_el
    

def serial_calc(mat, N):
    start = time.time()
    col = lambda i: [mat[k][i] for k in range(N)]
    for i in range(N):
        min = None
        for el in col(i):
            if min is None:
                min = el
            elif el < min:
                min = el
        mat[N-i-1][i] = min
    end = time.time()
    TIME_OF_SERIAL = end - start
    print(f"TIME_OF_SERIAL: {TIME_OF_SERIAL}")
    return TIME_OF_SERIAL
        
def beatiful_print(mat, N):
    print("\n")
    for i in range(N):
        for j in range(N):
            print(mat[i][j], end=" ")
        print("\n")


def parallel_execution(NUMBER_OF_WORKERS, SIZE, mat):
    start = time.time()
    q = Queue()
    result_queue = Queue()
    col = lambda i: [mat[k][i] for k in range(SIZE)]
    for i in range(SIZE):
        q.put((col(i), i))

    thread_arr = [None]*NUMBER_OF_WORKERS

    for i in range(NUMBER_OF_WORKERS):
        thread_arr[i] = Thread(target=worker, args=(q, result_queue))
        thread_arr[i].start()

    for i in range(NUMBER_OF_WORKERS):
        thread_arr[i].join()
    
    threads = [None]*NUMBER_OF_WORKERS

    for i in range(NUMBER_OF_WORKERS):
        threads[i] = Thread(target=matrix, args=(result_queue, mat, SIZE))
        threads[i].start()

    for i in range(NUMBER_OF_WORKERS):
        threads[i].join()

    end = time.time()

    TIME_OF_PARALLEL = end - start
    print(f"TIME_OF_PARALLEL: {TIME_OF_PARALLEL}")
    return TIME_OF_PARALLEL



    
if __name__ == '__main__':

    sum_par = 0
    sum_ser = 0

    for _ in range(10):
        i = random.randint(3, 1000)
        j = random.randint(2, 8)
        print(f"\n{i}-size, {j}-number of workers")
        matrix1 = fill_in_matrix(i)
        matrix2 = matrix1  
        t_par = parallel_execution(j, i, matrix1)
        sum_par += t_par
        t_ser = serial_calc(matrix2, i)
        sum_ser += t_ser
    
    print(f"\nTotal time of parallel execution {sum_par}")
    print(f"Total time of serial execution {sum_ser}")
   
