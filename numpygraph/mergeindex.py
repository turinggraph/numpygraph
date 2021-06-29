import time

import numpy as np


def merge_mem_array(mems):
    """ 合并 memmap
    """
    assert len(mems) > 0
    arr = np.zeros(shape=(sum([m.shape[0] for m in mems]),), dtype=mems[0].dtype)
    cur = 0
    for m in mems:
        arr[cur:cur + m.shape[0]] = m
        cur += m.shape[0]
    return arr


def unique_inplace(arr, unique=None, order=None, axis=0, kind='mergesort'):
    # memmap sort inplaced then unique index & counts return
    arr.sort(order=order, axis=axis, kind=kind)
    return np.unique(arr[unique], return_index=True, return_counts=True)


class MergeIndex:
    def __init__(self):
        self.edge_to_cursor = 0
        self.toarrconcat: np.memmap = None
        self.freq_idx_pointer = {}

    def merge_idx_to(self, k, ipts):
        mems = [np.memmap(f, mode='r', dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)])
                for f in ipts]
        arr = merge_mem_array(mems)
        _val, _idx, _len = unique_inplace(arr, unique='from', order=['from', 'ts'])
        return k, ipts, arr, _val, _idx, _len

    def merge_idx_to_callback(self, args):
        k, ipts, arr, _val, _idx, _len = args
        # TODO: 语意不明确
        graph = "/".join(ipts[0].split("/")[:-2])
        idxarr = np.memmap(f"{graph}/edges_sort/{k}.idx.arr",
                           mode='w+',
                           order='F',
                           shape=(_val.shape[0],),
                           dtype=[('value', np.int64),
                                  ('index', np.int64),
                                  ('length', np.int32)])
        idxarr['value'] = _val
        idxarr['index'] = _idx + self.edge_to_cursor
        idxarr['length'] = _len

        self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + arr.shape[0]]['value'] = arr['to']
        self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + arr.shape[0]]['ts'] = arr['ts']
        self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + arr.shape[0]]['index'] = arr[
            'from']
        self.edge_to_cursor += arr.shape[0]

    @staticmethod
    def merge_freq_idx_to(self, k, ipts):
        # 针对高频节点边表merge处理
        mems = [np.memmap(f, mode='r', dtype=[('to', np.int64), ('ts', np.int32)])
                for f in ipts]
        return k, mems
        pass

    @staticmethod
    def merge_freq_idx_to_callback(self, args):
        # 针对高频节点边表merge处理，回调
        k, mems = args
        value = k
        index = self.edge_to_cursor
        length = sum([m.shape[0] for m in mems])
        self.freq_idx_pointer[value] = (index, length)
        for m in mems:
            self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + m.shape[0]]['value'] = m['to']
            self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + m.shape[0]]['ts'] = m['ts']
            self.toarrconcat[self.edge_to_cursor: self.edge_to_cursor + m.shape[0]]['index'] = k
            self.edge_to_cursor += m.shape[0]
        pass

    def freq_idx_pointer_dump(self, context):
        # 针对高频节点->边索引表的处理
        # TODO: 针对高频节点为空的异常处理
        stime = time.time()
        print(f"Executing freq_idx_pointer_dump, starting at {stime}")
        if len(self.freq_idx_pointer) != 0:
            idxarr = np.memmap(f"{context.graph}/edges_sort/hid_freq.idx.arr",
                               mode='w+',
                               order='F',
                               shape=(len(self.freq_idx_pointer),),
                               dtype=[('value', np.int64),
                                      ('index', np.int64),
                                      ('length', np.int32)])

            for idx, (value, (index, length)) in enumerate(self.freq_idx_pointer.items()):
                idxarr[idx]['value'] = value
                idxarr[idx]['index'] = index
                idxarr[idx]['length'] = length
        etime = time.time()
        print(f"Finished freq_idx_pointer_dump, ends at {etime}")
        return 1
