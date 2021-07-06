import json
import os
import re
import numpy as np
from itertools import compress
import glob

from numpygraph.core.parse import Parse


class Context:
    """
    Context class that stores information required through the loading process. Information stored inside includes:
     * dataset: path to the dataset folder where all node and relation files lies
     * graph: path to the graph folder where all intermediate result rests
     * relation_files: list of relation files in the dataset folder
     * node_files: list of node files in the dataset folder (this and relation_files are for solving file dependencies)
     * NODE_TYPE: dict of node types to their hash values
     * NODE_FILE: dict of node files to hash values
     * HASH_NODE_TYPE: dict of hash values of node types to node types
     * HASH_SHORT_NODE_TYPE: dict of short hash values of node types to node typescript
     * HASH_NODE_FILE: dict of hash values of node files to node files
     * node_attr_type: dict of node type to their attributes' types
     * node_attr_type_without_str: dict of node type to their attributes' type, without str type, which cannot be put into :class:`numpygraph.core.arraylist` or :class:`numpygraph.core.arraydict`. We would record file pointers to str type.
     * node_attr_name and node_attr_name_without_str: dict of node type to their attributes' names, str excluded in the latter for the same reason as above.
     * valid_attrs: dict of node type to list of attributes's types they own
     * edge_attr_type, edge_attr_name: similar as those of nodes'
     * node_type_path, node_file_path: file path to json file that stores NODE_TYPE and NODE_FILE for next time's loading
     * closed: mark the end of context loading process
     * node_attr_chunk_num, node_csv_chunk_num: not used yet
    """

    def __init__(self):
        self.dataset = ""
        self.graph = ""
        self.relation_files = []
        self.node_files = []
        self.NODE_TYPE = {}
        self.NODE_FILE = {}
        self.HASH_NODE_TYPE = {}
        self.HASH_SHORT_NODE_TYPE = {}
        self.HASH_NODE_FILE = {}
        self.node_attr_type = {}
        self.node_attr_type_without_str = {}
        self.node_attr_name = {}
        self.node_attr_name_without_str = {}
        self.valid_attrs = {}
        self.edge_attr_type = {}
        self.edge_attr_name = {}
        self.node_type_path = None
        self.node_file_path = None
        self.closed = False
        self.node_attr_chunk_num = {}
        self.node_csv_chunk_num = {}

    def load_loc(self, dataset_path, graph_path):
        self.dataset = dataset_path
        self.graph = graph_path
        return self

    def load_context(self, dataset_path, graph_path):
        """
        Function that loads context (and closes it after finished).

        :type dataset_path: str
        :param dataset_path: path to dataset of node csv files and relation csv files
        :type graph_path: str
        :param graph_path: path to intermediate result path

        :return: self
        """
        self.load_loc(dataset_path, graph_path)
        self.load_node_type_hash()
        self.load_node_file_hash()
        self.prepare_nodes()
        self.prepare_relations()
        self.close()
        return self

    def prepare_nodes(self):
        """Build index of all node files
        """
        self.node_files = glob.glob(f"{self.dataset}/node_*.csv")
        for fn in self.node_files:
            # Context.count_node(fn)
            self.node_file_hash(fn)
        with open(self.node_file_path, "w") as f:
            f.write(json.dumps(self.NODE_FILE))
        return self

    def prepare_relations(self):
        """Build index of all relation files
        """
        self.relation_files = glob.glob(f"{self.dataset}/relation_*.csv")
        for fn in self.relation_files:
            with open(fn) as f:
                header_line = f.readline()
                FROM_COL, TO_COL = re.findall(r"\((.+?)\)", header_line)
                self.node_type_hash(FROM_COL)
                self.node_type_hash(TO_COL)
                edge_type = FROM_COL + TO_COL
                attr_name = []
                attr_type = []
                for item in header_line.split(',')[2:]:
                    item = item.strip()
                    item_name = item.split(':')[0]
                    item_type = Parse.get_type(item.split(':')[1])
                    attr_name.append(item_name)
                    attr_type.append(item_type)
                self.edge_attr_name[edge_type] = attr_name
                self.edge_attr_type[edge_type] = attr_type

        with open(self.node_type_path, "w") as f:
            f.write(json.dumps(self.NODE_TYPE))
        return self

    def node_type_hash(self, col):
        """
        Return hash value of given node type.

        :type col: str
        :param col: node type

        :return: the corresponding hash value
        """
        if self.closed:
            raise Exception("Please use query_type_hash to fetch hash value")
        if col in self.NODE_TYPE:
            return self.NODE_TYPE[col]
        else:
            # TODO: current query did not make use of the "60" here and the "60" in the chash function of hash
            self.NODE_TYPE[col] = len(self.NODE_TYPE) << 60
            return self.NODE_TYPE[col]

    def node_file_hash(self, filename):
        # 给定文件路径返回对应fid
        # 单文件2**45 = 100G， 文件上限个数2**19 = 524288个 （ext3 inode 默认 个数上限
        if self.closed:
            raise Exception("Please use query_file_hash to fetch hash value")
        if filename in self.NODE_FILE:
            return self.NODE_FILE[filename]
        else:
            self.NODE_FILE[filename] = len(self.NODE_FILE) << (64 - 19)
            with open(filename) as f:
                header_line = f.readline()
                node_type = re.findall(r"\((.+?)\)", header_line)[0]
                attr_name = []
                attr_type = []
                for item in header_line.split(',')[1:]:
                    item = item.strip()
                    item_name = item.split(':')[0]
                    item_type = Parse.get_type(item.split(':')[1])
                    attr_name.append(item_name)
                    attr_type.append(item_type)
                self.valid_attrs[node_type] = list(map(lambda a: False if a is np.str else True, attr_type))
                self.node_attr_name[node_type] = attr_name
                self.node_attr_type[node_type] = attr_type
                self.node_attr_type_without_str[node_type] = list(
                    compress(attr_type, self.valid_attrs[node_type]))
                self.node_attr_name_without_str[node_type] = list(
                    compress(attr_name, self.valid_attrs[node_type]))
            return self.NODE_FILE[filename]

    def node_file(self, fid):
        filename = self.NODE_FILE.get(fid, None)
        if filename is None:
            return None
        return open(filename)
        pass

    def load_node_type_hash(self):
        """
        Load node type from previously stored json file (probably comming from a previous session).

        :return: self
        """
        self.node_type_path = f"{self.graph}/node_type_id.json"
        if not os.path.exists(self.node_type_path):
            os.makedirs(os.path.dirname(self.node_type_path), exist_ok=True)
            self.NODE_TYPE = {}
            return self
        self.NODE_TYPE = json.loads(open(self.node_type_path, "r").read())
        return self

    def load_node_file_hash(self):
        """
        Load node files from previous json file

        :return: self
        """
        self.node_file_path = f"{self.graph}/node_file_id.json"
        if not os.path.exists(self.node_file_path):
            os.makedirs(os.path.dirname(self.node_file_path), exist_ok=True)
            self.NODE_FILE = {}
            return self
        self.NODE_FILE = json.loads(open(self.node_file_path, "r").read())
        return self

    def close(self):
        """
        Denote that writing to Context is done, no more further changed to Context will be applied.

        :return: self
        """
        for (node_type, hash_value) in self.NODE_TYPE.items():
            self.HASH_NODE_TYPE[hash_value] = node_type
            self.HASH_SHORT_NODE_TYPE[hash_value >> 60] = node_type
        for (file_path, hash_value) in self.NODE_FILE.items():
            self.HASH_NODE_FILE[hash_value] = file_path
        self.closed = True
        return self

    def query_type(self, hash_value):
        return self.HASH_NODE_TYPE[hash_value]

    def query_type_hash(self, value):
        return self.NODE_TYPE[value]

    def query_file(self, hash_value):
        return self.HASH_NODE_FILE[hash_value]

    def query_file_hash(self, value):
        return self.NODE_FILE[value]

    def query_edge_attr(self, value):
        return self.edge_attr_type[value]

    def query_node_attr(self, value):
        return self.node_attr_type[value]

    def query_node_attr_type_without_str(self, value):
        return self.node_attr_type_without_str[value]

    def query_node_attr_name(self, value):
        return self.node_attr_name[value]

    def query_node_attr_name_without_str(self, value):
        return self.node_attr_name_without_str[value]

    def query_valid_attrs(self, value):
        return self.valid_attrs[value]

    def parse_node_type(self, node_id):
        type_hash_short = node_id >> 60
        return self.HASH_SHORT_NODE_TYPE[type_hash_short]
