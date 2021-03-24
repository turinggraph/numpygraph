import os


class SplitFile():
    '''分片文件
    '''

    @staticmethod
    def split(path, num, jump=0, dist_random=False, single_threshold=int(1024 ** 2)):
        '''dist_random: 文件是否采用随机切割
        True: 随机切割
        False: 均匀切割
        single_threshold: 文件小于该值不做切割
        '''
        f = open(path)
        for _ in range(jump):
            f.readline()
        size = os.path.getsize(path)
        if size < single_threshold:
            num = 1
        _froms = [int((i / num) * size) for i in range(num)]
        _tos = [f.seek(_f) and f.readline() and f.tell() for _f in _froms[1:]] + [None]
        return [(path, _f, _t) for _f, _t in zip(_froms, _tos)]

    def __init__(self, path, _from, _to):
        self.f = open(path)
        self._from = _from
        self._to = _to
        self.f.seek(self._from)
        self.cursor = self.f.tell()
        pass

    def __iter__(self):
        return self

    def __next__(self):
        l = self.f.readline()
        if l == '' or ((self._to is not None) and self.cursor > self._to):
            raise StopIteration
        self.cursor += len(l.encode('utf-8'))
        return l

    def tell(self):
        return self.cursor
