import numpy as np
import os


class ArrayList():
    """
    A list based on numpy memmap

    Similar to :class:`numpygraph.core.arraydict.arraylist`, we can use this as a normal list (with a few tweaks), while this class writes the list to file(s) on disk. Reading is also creating an ArrayList object, specifiying where to read from.

    :type path: str
    :param path: path to file(s) on disk
    :type chunk_size: int
    :param chunk_size: maximum number of list items in a single file on disk.
    :type dtype: dtype
    :param dtype: type of list item
    """

    # TODO:
    #         批量增加
    #         merge后表的读取

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

    def __flush(self):
        """
        Flush all items in the list to disk. When we add some item to ArrayList, they are put into a temporal list (Python builtin list) rather than directly written in list. After the number of items in the temporal list reaches chunk_size, the list will be automaticlly flushed. This will also be invoked when the ArrayList gets signal of end of writing. It's not recommended to manually call this function.

        :return: No return value
        """
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
        """
        Append value to end of list.

        Can be used as if this is the Python built-in list. When we add some item to ArrayList, they are put into a temporal list (Python builtin list) rather than directly written in list. After the number of items in the temporal list reaches chunk_size, the list will be automaticlly flushed. For this reason, there might be a small delay in some append actions.

        :type value: your own dtype
        :param value: value to append
        :return: No return value
        """
        array_index = self.tail_cursor // self.chunk_size
        array_local_cursor: int = self.tail_cursor % self.chunk_size
        if array_index + 1 > len(self.arrays):
            # 首先将前一个array 替换为 memmap 到磁盘里
            self.__flush()
            self.arrays.append(np.zeros(dtype=self.dtype,
                                        shape=(self.chunk_size, 1)))
            self.array_current = self.arrays[array_index]
            self.array_cursor += 1
        self.array_current[array_local_cursor] = value
        # self.arrays[array_index][array_local_cursor] = value
        self.tail_cursor += 1

    def close(self, merge=True):
        """
        Close the current ArrayList.

        When closing, we would flush all list items in cache list to file on disk. And if merge is set to True, all files on folder written by this ArrayList would be merged into a single numpy memmap file. Otherwise, we would perform no operation.

        :type merge: boolean
        :param merge: whether to merge all small chunks to a single memmap file.
        :return: list of files the ArrayList has wrote to disk
        """
        if self.tail_cursor == 0:
            return []
        if merge is False:
            self.__flush()
            return [f"{self.path}.{i}" for i in range(len(self.arrays))]
        else:
            self.__flush()
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
            return [f"{self.path}"]

    def __getitem__(self, idx):
        return self.arrays[idx // self.chunk_size][idx % self.chunk_size]

    def __setitem__(self, idx, value):
        self.arrays[idx // self.chunk_size][idx % self.chunk_size] = value

    def path(self):
        return self.path
