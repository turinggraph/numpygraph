# export MALLOC_MMAP_THRESHOLD_=1000000 fixed bug

import numpy as np
import dask
from dask.distributed import Client
from dask.distributed import fire_and_forget
from dask.distributed import Client, LocalCluster
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool
import threading
import gc
import objgraph

import os, psutil
process = psutil.Process(os.getpid())
print(process.memory_info())
# class C:
#     def __init__(self):
#         self.var = np.zeros(500000)


def task(i):
    _ = [np.ones(500000) for i in range(200)]
    # for e in _:
    #     del e
    # del _
    return i


# from guppy import hpy

# h = hpy()
# print(h.heap())
# print(threading.enumerate())

# objgraph.show_growth()
pool = ThreadPool(10)
res = pool.starmap(task, [(i,) for i in range(100)])
pool.close()
gc.collect()
print(process.memory_info())
# objgraph.show_growth()
# print(h.heap())

# objgraph.show_backrefs(
#     objgraph.by_type("builtin_function_or_method")[0],
#     max_depth=10,
#     filename="obj.dot",
# )
# # for i in range(100):
# #     th = threading.Thread(target=task,daemon=True, args=(i, ))
# #     th.start()

# gc.collect()
# print(threading.enumerate())

# def test():
#     # cluster = LocalCluster()
#     # client = Client("tcp://127.0.0.1:36597")
#     client = Client(processes=True)
#     futures = [client.submit(task, i) for i in range(100)]
#     res = []
#     for f in futures:
#         # fire_and_forget(f)
#         res.append(f.result())
#     # res = [f.result() for f in futures]


# def test_process():
#     with Pool(4) as pool:
#         res = pool.starmap(task, [(i, ) for i in range(100)])
#     return res

# def test_thread():
# pool = ThreadPool(4)
# res = pool.starmap(task, [(i, ) for i in range(100)])
#     return res

# pool = Pool(4)
# res = pool.starmap(task, [(i, ) for i in range(100)])

# if __name__ == "__main__":
#     test()
