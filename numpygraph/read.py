import numpy as np

from numpygraph.core.arraydict import ArrayDict
from numpygraph.core.hash import chash


class Read:
    def __init__(self, dataset_loc, graph_loc, context):
        self.SHORT_HASH_MASK = (1 << 6) - 1
        self.graph = graph_loc
        self.dataset = dataset_loc
        self.context = context

    def fetch_node_id(self, node_type, node_value):
        """
        Return node_id (hash of node value) based on node value
        """
        node_type_hash = self.context.query_type_hash(node_type)
        node_hash = chash(node_type_hash, node_value)
        return node_hash

    def fetch_node_attr(self, node_id):
        """
        Return node attribute given node_id
        """
        node_type = self.context.parse_node_type(node_id)
        adict = ArrayDict(memmap_path=f"{self.graph}/nodes_mapper/{node_type}.dict.arr",
                          value_dtype=[('cursor', np.int64), ('chunk_id', np.int64), ('local_cursor', np.int64)],
                          memmap_mode='r')
        node_id_asarray = np.asarray([node_id])
        cursors = adict[node_id_asarray]
        cursor = cursors[0][0]
        chunk_id = cursors[0][1]
        local_cursor = cursors[0][2]
        data_type = [('nid', np.int64), ('cursor', np.int64), ('chunk_id', np.int64), ('local_cursor', np.int64)]
        data_type.extend(
            list(zip(self.context.query_node_attr_name_without_str(node_type),
                     self.context.query_node_attr_type_without_str(node_type)))
        )
        alist = np.memmap(filename=f"{self.graph}/node_{node_type}.csv.curarr/hid_{node_type}.curarr.chunk_{chunk_id}",
                          dtype=data_type
                          )
        return alist[local_cursor]

    def fetch_edge_attr(self, fid, tid):
        """
        Given from node id, to node id, return all edges attribute connecting the two nodes.

        Treat all edges as two-way edges.
        """
        # 暂时当作双向边处理
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
        """
        Return all neighbor nodes given a node's id
        """
        neighbor_edges = self.fetch_node_neibor_edges(_id)
        neighbor_nodes = []
        for edge in neighbor_edges:
            neighbor_nodes.append(edge[1])
        return neighbor_nodes

    def fetch_node_neibor_edges(self, _id):
        """
        Return all edges incident to the given node
        """
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
        """
        Given a node and degree to take in each step, do a three-level depth search that includes at most `degrees` number of nodes in each level then return all nodes traversed.
        """
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
        """
        Given a node id, return `size` random edges incident to the node as well as the nodes at the other side of the edges.

        :type size: int
        :param size: number of nodes (and corresponding edges) to return
        """
        selected_edges = self.fetch_node_neibor_edges(_id)[0:size]
        selected_nodes = []
        for edge in selected_edges:
            selected_nodes.append(edge[1])
        return selected_nodes, selected_edges
