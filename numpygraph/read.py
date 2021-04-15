import numpy as np

from numpygraph.core.arraydict import ArrayDict
from numpygraph.context import Context
from numpygraph.core.hash import chash


class Read:
    def __init__(self, dataset_loc, graph_loc):
        self.SHORT_HASH_MASK = (1 << 6) - 1
        self.graph = graph_loc
        self.dataset = dataset_loc

    def fetch_node_id(self, node_type, node_value):
        node_type_hash = Context.query_type_hash(node_type)
        node_hash = chash(node_type_hash, node_value)
        return node_hash

    def fetch_node_attr(self, _id):
        node_id = _id
        node_short_id = node_id & self.SHORT_HASH_MASK
        adict = ArrayDict(memmap_path=f"{self.graph}/nodes_mapper/hid_{node_short_id}.dict.arr",
                          value_dtype=[('cursor', np.int64), ('line_num', np.int64)], memmap_mode='r')
        node_id_asarray = np.asarray([node_id])
        cursors = adict[node_id_asarray]
        cursor = cursors[0][0]
        node_type = Context.parse_node_type(node_id)
        with open(f"{self.dataset}/node_{node_type}.csv") as f:
            try:
                f.seek(cursor)
                node_info = f.readline()
                if self.fetch_node_id(node_type, node_info.split(',')[0]) == node_id:
                    return node_info.strip('\n')
            except ValueError:
                pass

    def fetch_edge_attr(self, fid, tid):
        """
        From node id, to node id
        暂时当作双向边处理
        """
        fs_id = fid & self.SHORT_HASH_MASK
        ts_id = tid & self.SHORT_HASH_MASK
        f_value_asarray = np.asarray([fs_id, fid])
        # from value as array
        # from/to short id
        adict = ArrayDict(memmap_path=f"{self.graph}/edges_mapper/hid_{fs_id}.dict.arr",
                          value_dtype=[('index', np.int64), ('length', np.int32)], memmap_mode='r')
        loc = adict[f_value_asarray]
        toarrconcat = np.memmap(f"{self.graph}/concat.to.arr",
                                mode='r',
                                order='F',
                                dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])
        edges = []
        for (index, length) in loc:
            for i in range(length):
                if toarrconcat[index + i][0] == fid and toarrconcat[index + i][1] == tid:
                    edges.append(toarrconcat[index + i])
        return edges

    def fetch_node_neibor_nodes(self, _id):
        neighbor_edges = self.fetch_node_neibor_edges(_id)
        neighbor_nodes = []
        for edge in neighbor_edges:
            neighbor_nodes.append(edge[1])
        return neighbor_nodes

    def fetch_node_neibor_edges(self, _id):
        node_id = _id
        node_short_id = node_id & self.SHORT_HASH_MASK
        f_value_asarray = np.asarray([node_short_id, node_id])
        adict = ArrayDict(memmap_path=f"{self.graph}/edges_mapper/hid_{node_short_id}.dict.arr",
                          value_dtype=[('index', np.int64), ('length', np.int32)], memmap_mode='r')
        loc = adict[f_value_asarray]
        toarrconcat = np.memmap(f"{self.graph}/concat.to.arr",
                                mode='r',
                                order='F',
                                dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])
        edges = []
        for (index, length) in loc:
            edges.extend(toarrconcat[index:index + length])
        return edges

    def sample_node_with_degree(self, _id, degrees=None):
        if degrees is None:
            degrees = [4, 4, 4]
        nodes, edges = set(), set()
        extended_nodes = set()
        extended_edges = set()
        nodes.add(_id)
        for degree in degrees:
            for node in nodes:
                selected = self.random_sample_nodes(node, degree)
                extended_nodes.update(selected[0])
                extended_edges.update(selected[1])
            nodes.update(extended_nodes)
            edges.update(extended_edges)
        return nodes, edges

    def random_sample_nodes(self, _id, size=0):
        selected_edges = self.fetch_node_neibor_edges(_id)[0:size]
        selected_nodes = []
        for edge in selected_edges:
            selected_nodes.append(edge[1])
        return selected_nodes, selected_edges
