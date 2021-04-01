import numpy as np


class HashSplitDict():
    """分块字典集中管理
    """

    def __init__(self, path, dtype=np.int32, hlen=64):
        self.dtype = dtype
        self.hlen = hlen
        self.hdict = self._load(path)

    def _load(self, path):
        r = {}
        for i in range(self.hlen):
            r[i] = ArrayDict(memmap_path=f"{path}/hid_%d.dict.arr" % i,
                             memmap=True,
                             value_dtype=self.dtype,
                             memmap_mode='r')
        return r

    def __setitem__(self, key, value):
        _id = key % self.hlen
        for i in np.unique(_id):
            idx = (_id == i)
            self.hdict[i][key[idx]] = value[idx]
        pass

    def __getitem__(self, key):
        ret = np.zeros_like(key, dtype=self.dtype)
        _id = key % self.hlen
        for i in np.unique(_id):
            idx = (_id == i)
            ret[idx] = self.hdict[i][key[idx]]
        return ret


class ArrayDict():
    """基于numpy memmap 实现的字典
    """
    WITHOUT_NEXT_MAGIC = -1
    CONFIG_CHAIN_CURSOR = -1
    CONFIG_ITEM_SIZE = -2
    CONFIG_HASH_HEAP_SIZE = -3
    # Just for convenience, store the file_path here
    file_path = ""

    def __init__(self, item_size=None, hash_heap_rate=0.5, memmap=False, memmap_path=None,
                 memmap_mode='r+', value_dtype=np.int32, item_get_cursor=False):
        self.file_path = memmap_path
        # 在HEAP最尾部的三个值约定为(chain_cursor, item_size, HASH_HEAP_SIZE)
        # 如果不给item_size, 说明是读模式
        self.item_get_cursor = item_get_cursor
        # Critical problem here.
        # This is what we previously did:
        # if item_size is None:
        #     read self.HEAP
        #     set item_size to size of self.HEAP (something not None)
        # Do something else
        # if item_size is not None:
        #     set self.HEAP to an empty memmap
        #
        # Quite obviously, we can never read any ArrayDict from the disk this way
        is_New = item_size is not None
        if not is_New:
            self.HEAP = np.memmap(filename=memmap_path,
                                  dtype=[('key', np.int64),
                                         ('value', value_dtype),
                                         ('next', np.int32)],
                                  mode=memmap_mode)
            # print(self.HEAP)
            item_size = self.HEAP[self.CONFIG_ITEM_SIZE][0]
            hash_heap_size = self.HEAP[self.CONFIG_HASH_HEAP_SIZE][0]
        if 'hash_heap_size' in vars():
            self.HASH_HEAP_SIZE = hash_heap_size
        else:
            self.HASH_HEAP_SIZE = int(item_size * hash_heap_rate) + 1
        # 在 origin_size / hash_heap_size == 2时, 平均碰撞次数为2.38, 空间利用率为 0.85
        # 在 origin_size / hash_heap_size == 1时, 平均碰撞次数为1.57, 空间利用率为 0.64
        # 预留 floor(2.38 - 1) + 1的链表空间
        self.CHAIN_HEAP_SIZE = int((1 / hash_heap_rate) * self.HASH_HEAP_SIZE)
        self.ORIGIN_HASH_HEAP_SIZE_DIV_10 = (2 ** (8 * 8)) // 10
        self.SCALE = int(self.ORIGIN_HASH_HEAP_SIZE_DIV_10 / (self.HASH_HEAP_SIZE / 10))
        self.ZERO_BIAS = self.HASH_HEAP_SIZE // 2 + 1
        self.value_dtype = value_dtype
        if is_New:  # 根据 item_size 新建  # second part of the critical problem
            if not memmap:
                self.HEAP = np.zeros((self.HASH_HEAP_SIZE + self.CHAIN_HEAP_SIZE),
                                     dtype=[('key', np.int64), ('value', self.value_dtype),
                                            ('next', np.int32)])
            else:
                self.HEAP = np.memmap(memmap_path,
                                      order='C',
                                      dtype=[('key', np.int64), ('value', self.value_dtype),
                                             ('next', np.int32)],
                                      mode=memmap_mode, shape=(self.HASH_HEAP_SIZE + self.CHAIN_HEAP_SIZE))

        # 保证链尾巴的原子性, 这块会有部分性能损耗
        if self.HEAP[self.CONFIG_CHAIN_CURSOR][0] == 0:
            self.HEAP[self.CONFIG_CHAIN_CURSOR][0] = self.HASH_HEAP_SIZE
            self.HEAP[self.CONFIG_HASH_HEAP_SIZE][0] = self.HASH_HEAP_SIZE
            self.HEAP[self.CONFIG_ITEM_SIZE][0] = item_size

    def __setitem__(self, key, value):
        expect_cursor = (key // self.SCALE) + self.ZERO_BIAS
        # 解决batch insert hash冲突问题, 强制将有冲突的hash分成多个batch插入
        while len(expect_cursor) != 0:
            _, current_index = np.unique(expect_cursor, return_index=True)
            self.set_value(expect_cursor[current_index], key[current_index], value[current_index])
            expect_cursor = np.delete(expect_cursor, current_index)
            key = np.delete(key, current_index)
            value = np.delete(value, current_index)
        return True

    def __getitem__(self, key):
        # expect_cursor = (key % self.HASH_HEAP_SIZE)
        expect_cursor = (key // self.SCALE) + self.ZERO_BIAS
        return self.get_value(expect_cursor, key)

    def get_value(self, cursor, key):
        ret = np.zeros_like(key, dtype=self.value_dtype)
        if self.item_get_cursor:
            ret_cursor = np.zeros_like(key, dtype=np.int32)
        heap = self.HEAP[cursor]
        cursor_key, cursor_value, cursor_next = heap['key'], heap['value'], heap['next']
        first_access_idx = np.where(cursor_next == 0)[0]
        self_hosted_idx = np.where(cursor_key == key)[0]
        next_new_idx = np.where((cursor_key != key) & (cursor_next == self.WITHOUT_NEXT_MAGIC))[0]
        next_access_idx = np.where((cursor_key != key)
                                   & (cursor_next != self.WITHOUT_NEXT_MAGIC)
                                   & (cursor_next != 0))[0]
        #
        if len(first_access_idx) > 0:
            ret[first_access_idx] = -1
        #
        if len(self_hosted_idx) > 0:
            ret[self_hosted_idx] = cursor_value[self_hosted_idx]
            if self.item_get_cursor:
                ret_cursor[self_hosted_idx] = cursor[self_hosted_idx]

        #
        if len(next_new_idx) > 0:
            ret[first_access_idx] = -1
        #
        if len(next_access_idx) > 0:
            if self.item_get_cursor:
                ret[next_access_idx], ret_cursor[next_access_idx] = \
                    self.get_value(cursor_next[next_access_idx], key[next_access_idx])
            else:
                ret[next_access_idx] = self.get_value(cursor_next[next_access_idx], key[next_access_idx])

        if self.item_get_cursor:
            return ret, ret_cursor

        return ret

    def set_value(self, cursor, key, value):
        """ 插入存在链表的新增关系， 这部分要保证 cursor 不允许重复
        """
        heap = self.HEAP[cursor]
        cursor_key, cursor_value, cursor_next = heap['key'], heap['value'], heap['next']
        first_access_idx = np.where(cursor_next == 0)[0]
        self_hosted_idx = np.where(cursor_key == key)[0]
        next_new_idx = np.where((cursor_key != key) & (cursor_next == self.WITHOUT_NEXT_MAGIC))[0]
        next_access_idx = np.where((cursor_key != key)
                                   & (cursor_next != self.WITHOUT_NEXT_MAGIC)
                                   & (cursor_next != 0))[0]

        # insert
        if len(first_access_idx) > 0:
            self.HEAP['key'][cursor[first_access_idx]] = key[first_access_idx]
            self.HEAP['value'][cursor[first_access_idx]] = value[first_access_idx]
            self.HEAP['next'][cursor[first_access_idx]] = self.WITHOUT_NEXT_MAGIC
        # update
        if len(self_hosted_idx) > 0:
            self.HEAP['value'][cursor[self_hosted_idx]] = value[self_hosted_idx]
        #
        if len(next_new_idx) > 0:
            new_idx = self.HEAP[self.CONFIG_CHAIN_CURSOR][0] \
                      + np.arange(len(next_new_idx))
            self.HEAP['key'][new_idx] = key[next_new_idx]
            self.HEAP['value'][new_idx] = value[next_new_idx]
            self.HEAP['next'][new_idx] = self.WITHOUT_NEXT_MAGIC
            self.HEAP['next'][cursor[next_new_idx]] = new_idx
            self.HEAP[self.CONFIG_CHAIN_CURSOR][0] += len(next_new_idx)
        #
        if len(next_access_idx) > 0:
            self.set_value(cursor_next[next_access_idx], key[next_access_idx], value[next_access_idx])
        return True
