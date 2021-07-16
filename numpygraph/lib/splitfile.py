import os


class SplitFile:
    """
    Split (csv) files into multiple parts with file cursors (smallest unit of split is a line), so that we can split dumping tasks, utilizing multiprocessing.
    """

    @staticmethod
    def split(path, num, jump=0, dist_random=False, single_threshold=int(1024 ** 2)):
        """
        Split file into parts, returning a list of file pointers (that points into the file).

        :type path: str
        :param path: path to target file being split
        :type num: int
        :param num: number of slices to split into
        :type jump: int
        :param jump: number of lines to jump (maybe because they are headers)
        :type dist_random: int
        :param dist_random: whether split files with random lines in each slice
        :type single_threshold: int
        :param single_threshold: least file size so that we split it 文件小于该值不做切割
        """
        f = open(path)
        for _ in range(jump):
            f.readline()
        size = os.path.getsize(path)
        # 强制让文件分片多一点，否则把节点太少时节点属性csv转成arraylist的时候就一块儿可能掩盖一些bug
        # if size < single_threshold:
        #     num = 1
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
        """
        Return current location of cursor in file.
        """
        return self.cursor
