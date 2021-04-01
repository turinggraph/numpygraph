import numpy as np
import os


class ArrayList():
    """基于numpy memmap 实现的列表
    TODO:
        批量增加
        merge后表的读取
    """

    def __init__(self, path, chunk_size=1000000, dtype=None):
        self.path = path
        self.chunk_size: int = chunk_size
        self.dtype = dtype
        self.arrays = []
        self.array_current: np.array = None
        self.tail_cursor: int = 0
        self.array_cursor = 0
        pass

    def __len__(self):
        return self.tail_cursor

    def flush(self):
        if len(self.arrays) != 0 and type(self.arrays[-1]) != np.memmap:
            array_usage_cursor = ((self.tail_cursor - 1) % self.chunk_size) + 1
            #             print(array_usage_cursor, self.chunk_size)
            array_memmap = np.memmap(self.path + ".%d" % (len(self.arrays) - 1),
                                     mode='w+',
                                     dtype=self.dtype,
                                     shape=(array_usage_cursor, 1))
            array_memmap[:] = self.arrays[-1][:array_usage_cursor]
            self.arrays[-1] = array_memmap

    def append(self, value):
        array_index = self.tail_cursor // self.chunk_size
        array_local_cursor: int = self.tail_cursor % self.chunk_size
        if array_index + 1 > len(self.arrays):
            # 首先将前一个array 替换为 memmap 到磁盘里
            self.flush()
            self.arrays.append(np.zeros(dtype=self.dtype,
                                        shape=(self.chunk_size, 1)))
            self.array_current = self.arrays[array_index]
            self.array_cursor += 1
        self.array_current[array_local_cursor] = value
        # self.arrays[array_index][array_local_cursor] = value
        self.tail_cursor += 1

    def close(self, merge=True):
        if self.tail_cursor == 0:
            return
        if merge is False:
            self.flush()
        else:
            self.flush()
            opt = np.memmap(self.path,
                            mode='w+',
                            dtype=self.dtype,
                            shape=(self.tail_cursor, 1))
            cur = 0
            for array in self.arrays:
                opt[cur: cur + min(array.shape[0], self.tail_cursor % self.chunk_size)] = \
                    array[:min(array.shape[0], self.tail_cursor % self.chunk_size)]
                cur += array.shape[0]

            for i in range(len(self.arrays)):
                os.remove(self.path + ".%d" % i)

    def __getitem__(self, idx):
        return self.arrays[idx // self.chunk_size][idx % self.chunk_size]

    def __setitem__(self, idx, value):
        self.arrays[idx // self.chunk_size][idx % self.chunk_size] = value

    def path(self):
        return self.path
