import json
import os
import re


class Context:
    NODE_TYPE = {}
    NODE_FILE = {}
    NODE_ATTR = {}
    NODE_NUM = {}
    node_type_path = None
    node_file_path = None

    # no "self" passed in, we only use one instance of Context

    # There's nothing about the type of the nodes' contents, it's all about their type and the relationship file hash
    def prepare_nodes(node_files):
        for fn in node_files:
            # Context.count_node(fn)
            Context.node_file_hash(fn)
        with open(Context.node_file_path, "w") as f:
            f.write(json.dumps(Context.NODE_FILE))

    def prepare_relations(relation_files):
        for fn in relation_files:
            with open(fn) as f:
                FROM_COL, TO_COL = re.findall("\((.+?)\)", f.readline())
                Context.node_type_hash(FROM_COL)
                Context.node_type_hash(TO_COL)
        with open(Context.node_type_path, "w") as f:
            f.write(json.dumps(Context.NODE_TYPE))

    def node_type_hash(col):
        if col in Context.NODE_TYPE:
            return Context.NODE_TYPE[col]
        else:
            Context.NODE_TYPE[col] = len(Context.NODE_TYPE) << 60
            return Context.NODE_TYPE[col]

    def node_file_hash(filename):
        # 给定文件路径返回对应fid
        # 单文件2**45 = 100G， 文件上限个数2**19 = 524288个 （ext3 inode 默认 个数上限）
        if filename in Context.NODE_FILE:
            return Context.NODE_FILE[filename]
        else:
            Context.NODE_FILE[filename] = len(Context.NODE_FILE) << (64 - 19)
            return Context.NODE_FILE[filename]

    def node_file(fid):
        filename = Context.NODE_FILE.get(fid, None)
        if filename is None:
            return None
        return open(filename)
        pass

    def load_node_type_hash(path):
        Context.node_type_path = path
        if not os.path.exists(Context.node_type_path):
            os.makedirs(os.path.dirname(Context.node_type_path), exist_ok=True)
            Context.NODE_TYPE = {}
            return
        Context.NODE_TYPE = json.loads(open(Context.node_type_path, "r").read())

    def load_node_file_hash(path):
        Context.node_file_path = path
        if not os.path.exists(Context.node_file_path):
            os.makedirs(os.path.dirname(Context.node_file_path), exist_ok=True)
            Context.NODE_FILE = {}
            return
        Context.NODE_FILE = json.loads(open(Context.node_file_path, "r").read())
