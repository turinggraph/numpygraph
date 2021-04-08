import json
import os
import re


class Context:
    NODE_TYPE = {}
    NODE_FILE = {}
    HASH_NODE_TYPE = {}
    HASH_SHORT_NODE_TYPE = {}
    HASH_NODE_FILE = {}
    node_type_path = None
    node_file_path = None
    closed = False

    @staticmethod
    def prepare_nodes(node_files):
        for fn in node_files:
            # Context.count_node(fn)
            Context.node_file_hash(fn)
        with open(Context.node_file_path, "w") as f:
            f.write(json.dumps(Context.NODE_FILE))

    @staticmethod
    def prepare_relations(relation_files):
        for fn in relation_files:
            with open(fn) as f:
                FROM_COL, TO_COL = re.findall(r"\((.+?)\)", f.readline())
                Context.node_type_hash(FROM_COL)
                Context.node_type_hash(TO_COL)
        with open(Context.node_type_path, "w") as f:
            f.write(json.dumps(Context.NODE_TYPE))

    @staticmethod
    def node_type_hash(col):
        if Context.closed:
            raise Exception("Please use query_type_hash to fetch hash value")
        if col in Context.NODE_TYPE:
            return Context.NODE_TYPE[col]
        else:
            Context.NODE_TYPE[col] = len(Context.NODE_TYPE) << 60
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

    @staticmethod
    def close():
        # Denote that writing to Context is done, no more further changed to Context will be applied.
        for (node_type, hash_value) in Context.NODE_TYPE.items():
            Context.HASH_NODE_TYPE[hash_value] = node_type
            Context.HASH_SHORT_NODE_TYPE[hash_value >> 60] = node_type
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
    def query_file(hash_value):
        return Context.HASH_NODE_FILE[hash_value]

    @staticmethod
    def query_file_hash(value):
        return Context.NODE_FILE[value]

    @staticmethod
    def parse_node_type(node_id):
        type_hash_short = node_id >> 60
        return Context.HASH_SHORT_NODE_TYPE[type_hash_short]
