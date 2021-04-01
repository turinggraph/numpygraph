import numpy as np

from numpygraph.core.arraydict import ArrayDict
from numpygraph.context import Context
from numpygraph.core.hash import chash


class Read:
    graph = ""
    dataset = ""

    def init(dataset_loc, graph_loc):
        Read.SHORT_HASH_MASK = (1 << 6) - 1
        Read.graph = graph_loc
        Read.dataset = dataset_loc

    def find_relationships(node_val, node_type=-1, node_type_hash=-1):
        if node_type == -1 and node_type_hash == -1:
            return -1
        else:
            if node_type_hash == -1:
                node_type_hash = Context.node_type_hash(node_type)
        node_hash = chash(node_type_hash, node_val)
        freq_val = node_hash
        infreq_val = node_hash & Read.SHORT_HASH_MASK
        value_asarray = np.asarray([freq_val, infreq_val])
        adict = ArrayDict(memmap_path=f"{Read.graph}/edges_mapper/hid_{infreq_val}.dict.arr",
                          value_dtype=[('index', np.int64), ('length', np.int32)], memmap_mode='r')
        loc = adict[value_asarray]

        toarrconcat = np.memmap(f"{Read.graph}/concat.to.arr",
                                mode='r',
                                order='F',
                                dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])
        relationships = []
        for (index, length) in loc:
            relationships.extend(np.copy(toarrconcat[index: index + length]))

        # print(result)
        return relationships

    def node_info(node_val, node_type=-1, node_type_hash=-1):
        if node_type == -1 and node_type_hash == -1:
            return ""
        else:
            if node_type_hash == -1:
                node_type_hash = Context.node_type_hash(node_type)
        node_hash = chash(node_type_hash, node_val)  # nid
        node_short_id = node_hash & Read.SHORT_HASH_MASK
        adict = ArrayDict(memmap_path=f"{Read.graph}/nodes_mapper/hid_{node_short_id}.dict.arr",
                          value_dtype=[('cursor', np.int64)], memmap_mode='r')
        print(adict.file_path)
        node_hash_asarray = np.asarray([node_hash])
        cursors = adict[node_hash_asarray]
        print(cursors)
        cursor = cursors[0][0]
        print(cursor)
        with open(f"{Read.dataset}/node_{node_type}.csv") as f:
            f.seek(cursor)
            node_info = f.readline()
        return node_info
