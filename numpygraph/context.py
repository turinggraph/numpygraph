import json
import os
import re
import numpy as np
from itertools import compress
import pickle

# from pydoc import locate

from numpygraph.core.parse import Parse


class Context:
    dataset = ""
    graph = ""
    NODE_TYPE = {}
    NODE_FILE = {}
    HASH_NODE_TYPE = {}
    HASH_SHORT_NODE_TYPE = {}
    HASH_NODE_FILE = {}
    node_attr_type = {}
    node_attr_type_without_str = {}
    node_attr_name = {}
    node_attr_name_without_str = {}
    valid_attrs = {}
    edge_attr_type = {}
    edge_attr_name = {}
    node_type_path = None
    node_file_path = None
    closed = False
    node_attr_chunk_num = {}
    node_csv_chunk_num = {}

    @staticmethod
    def load_loc(dataset_path, graph_path):
        Context.dataset = dataset_path
        Context.graph = graph_path

    @staticmethod
    def prepare_nodes(node_files):
        for fn in node_files:
            # Context.count_node(fn)
            Context.node_file_hash(fn)
        with open(Context.node_file_path, "w") as f:
            f.write(json.dumps(Context.NODE_FILE))
        with open(
            Context.node_file_path.replace(
                ".json", "_node_attr_name_without_str.json"
            ),
            "w",
        ) as f:
            f.write(json.dumps(Context.node_attr_name_without_str))
        with open(
            Context.node_file_path.replace(
                ".json", "_node_attr_type_without_str.json"
            ),
            "w",
        ) as f:
            f.write(
                json.dumps(
                    {
                        k: [Parse.get_type_reverse(t) for t in v]
                        for k, v in Context.node_attr_type_without_str.items()
                    }
                )
            )

    @staticmethod
    def prepare_relations(relation_files):
        for fn in relation_files:
            with open(fn) as f:
                header_line = f.readline()
                FROM_COL, TO_COL = re.findall(r"\((.+?)\)", header_line)
                Context.node_type_hash(FROM_COL)
                Context.node_type_hash(TO_COL)
                edge_type = FROM_COL + TO_COL
                attr_name = []
                attr_type = []
                for item in header_line.split(",")[2:]:
                    item = item.strip()
                    item_name = item.split(":")[0]
                    item_type = Parse.get_type(item.split(":")[1])
                    attr_name.append(item_name)
                    attr_type.append(item_type)
                Context.edge_attr_name[edge_type] = attr_name
                Context.edge_attr_type[edge_type] = attr_type

        with open(Context.node_type_path, "w") as f:
            f.write(json.dumps(Context.NODE_TYPE))

    @staticmethod
    def node_type_hash(col):
        if Context.closed:
            raise Exception("Please use query_type_hash to fetch hash value")
        if col in Context.NODE_TYPE:
            return Context.NODE_TYPE[col]
        else:
            # TODO: current query did not make use of the "60" here and the "60" in the chash function of hash
            # Context.NODE_TYPE[col] = len(Context.NODE_TYPE) << 60
            Context.NODE_TYPE[col] = len(Context.NODE_TYPE) << 59
            return Context.NODE_TYPE[col]

    @staticmethod
    def node_file_hash(filename):
        # 给定文件路径返回对应fid
        # 单文件2**45 = 100G， 文件上限个数2**19 = 524288个 （ext3 inode 默认 个数上限
        if Context.closed:
            raise Exception("Please use query_file_hash to fetch hash value")
        if filename in Context.NODE_FILE:
            return Context.NODE_FILE[filename]
        else:
            Context.NODE_FILE[filename] = len(Context.NODE_FILE) << (64 - 19)
            with open(filename) as f:
                header_line = f.readline()
                node_type = re.findall(r"\((.+?)\)", header_line)[0]
                attr_name = []
                attr_type = []
                for item in header_line.split(",")[1:]:
                    item = item.strip()
                    item_name = item.split(":")[0]
                    item_type = Parse.get_type(item.split(":")[1])
                    attr_name.append(item_name)
                    attr_type.append(item_type)
                Context.valid_attrs[node_type] = list(
                    map(lambda a: False if a is np.str else True, attr_type)
                )
                Context.node_attr_name[node_type] = attr_name
                Context.node_attr_type[node_type] = attr_type
                Context.node_attr_type_without_str[node_type] = list(
                    compress(attr_type, Context.valid_attrs[node_type])
                )
                Context.node_attr_name_without_str[node_type] = list(
                    compress(attr_name, Context.valid_attrs[node_type])
                )
            return Context.NODE_FILE[filename]

    @staticmethod
    def node_file(fid):
        filename = Context.NODE_FILE.get(fid, None)
        if filename is None:
            return None
        return open(filename)
        pass

    @staticmethod
    def load_node_type_hash(path):
        Context.node_type_path = path
        if not os.path.exists(Context.node_type_path):
            os.makedirs(os.path.dirname(Context.node_type_path), exist_ok=True)
            Context.NODE_TYPE = {}
            return
        Context.NODE_TYPE = json.loads(open(Context.node_type_path, "r").read())

    @staticmethod
    def load_node_file_hash(path):
        Context.node_file_path = path
        if not os.path.exists(Context.node_file_path):
            os.makedirs(os.path.dirname(Context.node_file_path), exist_ok=True)
            Context.NODE_FILE = {}
            return
        Context.NODE_FILE = json.loads(open(Context.node_file_path, "r").read())
        Context.node_attr_name_without_str = json.loads(
            open(
                Context.node_file_path.replace(
                    ".json", "_node_attr_name_without_str.json"
                ),
                "r",
            ).read()
        )
        Context.node_attr_type_without_str = {
            k: [Parse.get_type(e) for e in v]
            for k, v in json.loads(
                open(
                    Context.node_file_path.replace(
                        ".json", "_node_attr_type_without_str.json"
                    ),
                    "r",
                ).read()
            ).items()
        }

    @staticmethod
    def close():
        # Denote that writing to Context is done, no more further changed to Context will be applied.
        for (node_type, hash_value) in Context.NODE_TYPE.items():
            Context.HASH_NODE_TYPE[hash_value] = node_type
            Context.HASH_SHORT_NODE_TYPE[hash_value >> 59] = node_type
        for (file_path, hash_value) in Context.NODE_FILE.items():
            Context.HASH_NODE_FILE[hash_value] = file_path
        Context.closed = True

    @staticmethod
    def query_type(hash_value):
        return Context.HASH_NODE_TYPE[hash_value]

    @staticmethod
    def query_type_hash(value):
        return Context.NODE_TYPE[value]

    @staticmethod
    def query_type_hash_sync(value, path):
        Context.load_node_type_hash(path)
        return Context.NODE_TYPE[value]

    @staticmethod
    def query_file(hash_value):
        return Context.HASH_NODE_FILE[hash_value]

    @staticmethod
    def query_file_hash(value):
        return Context.NODE_FILE[value]

    @staticmethod
    def query_edge_attr(value):
        return Context.edge_attr_type[value]

    @staticmethod
    def query_node_attr(value):
        return Context.node_attr_type[value]

    @staticmethod
    def query_node_attr_type_without_str(value):
        return Context.node_attr_type_without_str[value]

    @staticmethod
    def query_node_attr_name(value):
        return Context.node_attr_name[value]

    @staticmethod
    def query_node_attr_name_without_str(value):
        return Context.node_attr_name_without_str[value]

    @staticmethod
    def query_valid_attrs(value):
        return Context.valid_attrs[value]

    @staticmethod
    def parse_node_type(node_id):
        # type_hash_short = node_id >> 60
        type_hash_short = node_id >> 59
        return Context.HASH_SHORT_NODE_TYPE[type_hash_short]
