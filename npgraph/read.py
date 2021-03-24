import numpy as np

from npgraph.tools.splitfile import SplitFile
from npgraph.tools.arraylist import ArrayList
from npgraph.tools.arraydict import ArrayDict
from npgraph.context import Context
from npgraph.tools.hash import chash


class Read:
    graph = ""
    dataset = ""

    def init(dataset_loc, graph_loc):
        Read.SHORT_HASH_MASK = (1 << 6) - 1
        Read.graph = graph_loc
        Read.dataset = dataset_loc

    def find_adjacent(node_val, node_type=-1, node_type_hash=-1):
        if node_type == -1 and node_type_hash == -1:
            return -1
        else:
            if node_type_hash == -1:
                node_type_hash = Context.node_type_hash(node_type)
        node_hash = chash(node_type_hash, node_val)
        freq_val = node_hash
        infreq_val = node_hash & Read.SHORT_HASH_MASK
        sorted_edges = np.memmap(f"{Read.graph}/concat.to.arr",
                                 mode='r',
                                 dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])

    def node_info(node_val, node_type=-1, node_type_hash=-1):
        if node_type == -1 and node_type_hash == -1:
            return ""
        else:
            if node_type_hash == -1:
                node_type_hash = Context.node_type_hash(node_type)
        node_hash = chash(node_type_hash, node_val)
        for chunk_num in range(64):
            print("memmap_path=", f"{Read.graph}/nodes_mapper/hid_%d.dict.arr" % chunk_num)
            adict = ArrayDict(memmap_path=f"{Read.graph}/edges_mapper/hid_%d.dict.arr" % chunk_num,
                              value_dtype=[('cursor', np.int64)])
            if node_hash in adict:
                cursor = adict[node_hash]['cursor']
            with open(f"{Read.graph}/node_*.csv.curarr/hid_%d_*.curarr.chunk*" % node_hash) as f:
                f.seek(cursor)
            node_info = f.readline()
            return node_info
        return ""
        pass
