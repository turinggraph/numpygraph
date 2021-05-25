import glob
import numpy as np
import time
import os
import re
from collections import defaultdict, Counter
import multiprocessing
from multiprocessing import Pool, cpu_count
import random
from itertools import compress

from numpygraph.lib.splitfile import SplitFile
from numpygraph.core.arraylist import ArrayList
from numpygraph.core.arraydict import ArrayDict
from numpygraph.core.hash import chash
from numpygraph.context import Context
from numpygraph.mergeindex import MergeIndex
from numpygraph.core.parse import Parse
import dask


def lines_sampler(relationship_files):
    """对图数据集做采样统计, 供后续估计hash空间使用, 提升导入效率"""
    key_sample_lines, nodes_line_num = defaultdict(list), defaultdict(int)
    for relation in relationship_files:
        relation, _ = os.path.abspath(relation), os.path.basename(relation)
        with open(relation) as f:
            line = f.readline()
            from_col, to_col = re.findall(r"\((.+?)\)", line)
        splitfiles = SplitFile.split(relation, num=200, jump=1)
        filesize = os.path.getsize(relation)

        def sample_lines(sfs, cnt=1000):
            for arg in sfs:
                sf, i = SplitFile(*arg), 0
                for line in sf:
                    if i > cnt:
                        break
                    yield line
                    i += 1

        lines = list(sample_lines(splitfiles))
        for line in lines:
            keys = line.replace("\n", "").split(Context.DELIMITER)
            if len(keys) != 3:
                continue
            key_sample_lines[from_col].append(keys[0])
            key_sample_lines[to_col].append(keys[1])
        line_len_mean = np.mean(
            list(map(lambda l: len(l.encode("utf-8")), lines))
        )
        line_num = int(filesize / line_len_mean)
        nodes_line_num[from_col] += line_num
        nodes_line_num[to_col] += line_num
    return key_sample_lines, nodes_line_num


def node_hash_space_stat(
    key_sample_lines,
    nodes_line_num,
    without_freq=False,
    HID_BATCH_SIZE_AVERAGE=int(5e6),
    FREQ_WORDS_TOP_KEEP=10,
    FREQ_WORDS_TOP_RATIO=0.01,
    graph="",
):
    """根据数据集的分布情况自动切分hash空间和高频节点列表"""
    NODES_SHORT_HASH, freq_nodes = {}, {}
    for node, l in nodes_line_num.items():
        NODES_SHORT_HASH[node] = l // HID_BATCH_SIZE_AVERAGE
    if without_freq:
        return freq_nodes, NODES_SHORT_HASH

    for node, words in key_sample_lines.items():
        node_freq_word_cnt = Counter(words).most_common(FREQ_WORDS_TOP_KEEP)
        csum = len(words)
        topstat = [
            (node, fcnt, fcnt / csum) for node, fcnt in node_freq_word_cnt
        ]
        freqitem = list(filter(lambda x: x[2] > FREQ_WORDS_TOP_RATIO, topstat))
        freqrate = sum([e[-1] for e in freqitem])
        # HASH空间切分数 = 非高频节点的记录数 / 期望的hash batch大小
        NODES_SHORT_HASH[node] = (
            int(nodes_line_num[node] * (1 - freqrate)) // HID_BATCH_SIZE_AVERAGE
        )
        # 建立节点高频word集合
        if len(freqitem) > 0:
            freq_nodes[node] = set(
                [
                    chash(
                        Context.query_type_hash_sync(
                            node, f"{graph}/node_type_id.json"
                        ),
                        item[0],
                    )
                    for item in freqitem
                ]
            )
    return freq_nodes, NODES_SHORT_HASH


@dask.delayed
def lines2idxarr(
    output, splitfile_arguments, chunk_id, freq_nodes, NODES_SHORT_HASH, graph
):
    # output, (path, _from, _to), chunk = args
    with open(splitfile_arguments[0]) as f:
        FROM_COL, TO_COL = re.findall(r"\((.+?)\)", f.readline())
        FROM_COL_HASH, TO_COL_HASH = (
            Context.query_type_hash_sync(
                FROM_COL, f"{graph}/node_type_id.json"
            ),
            Context.query_type_hash_sync(TO_COL, f"{graph}/node_type_id.json"),
        )
    # FROM_SHORT_HASH, TO_SHORT_HASH = NODES_SHORT_HASH[FROM_COL], NODES_SHORT_HASH[TO_COL]
    # 固定为64的原因主要还是考虑后续会映射到edge dict中, 统一使用64bin去切割
    FROM_SHORT_HASH, TO_SHORT_HASH, SHORT_HASH_MASK = 64, 64, (1 << 6) - 1

    splitfile = SplitFile(*splitfile_arguments)
    os.makedirs(output, exist_ok=True)

    # 随机块大小是为了让进程吃资源的节奏错开
    def random_chunk_size():
        return random.randint(300000, 600000)

    # 针对非高频节点使用使用短hash映射到共享空间，需要独立重排
    from_node_lists = [
        ArrayList(
            "%s/hid_%d_%s.idxarr.chunk_%d" % (output, i, FROM_COL, chunk_id),
            chunk_size=random_chunk_size(),
            dtype=[("from", np.int64), ("to", np.int64), ("ts", np.int32)],
        )
        for i in range(FROM_SHORT_HASH)
    ]
    to_node_lists = [
        ArrayList(
            "%s/hid_%d_%s.idxarr.chunk_%d" % (output, i, TO_COL, chunk_id),
            chunk_size=random_chunk_size(),
            dtype=[("from", np.int64), ("to", np.int64), ("ts", np.int32)],
        )
        for i in range(TO_SHORT_HASH)
    ]
    # 针对高频节点， 使用独立的空间， 不需要重排
    freq_output = f"{output}/freq_edges"
    os.makedirs(freq_output, exist_ok=True)
    ffdict_able_flag, tfdict_able_flag = False, False
    if FROM_COL in freq_nodes:
        from_node_freq_dict = {
            fk: ArrayList(
                "%s/hid_%s_%s.idxarr.chunk_%d"
                % (freq_output, str(fk), FROM_COL, chunk_id),
                chunk_size=random_chunk_size(),
                dtype=[("to", np.int64), ("ts", np.int32)],
            )
            #   dtype=[('from', np.int64), ('to', np.int64)])
            for fk in list(freq_nodes[FROM_COL])
        }
        from_node_freq_dict_set = set(from_node_freq_dict.keys())
        ffdict_able_flag = True
    if TO_COL in freq_nodes:
        to_node_freq_dict = {
            tk: ArrayList(
                "%s/hid_%s_%s.idxarr.chunk_%d"
                % (freq_output, str(tk), TO_COL, chunk_id),
                chunk_size=random_chunk_size(),
                dtype=[("to", np.int64), ("ts", np.int32)],
            )
            #   dtype=[('from', np.int64), ('to', np.int64)])
            for tk in list(freq_nodes[TO_COL])
        }
        to_node_freq_dict_set = set(to_node_freq_dict.keys())
        tfdict_able_flag = True

    for line in splitfile:
        # from_str, to_str
        r = line[:-1].split(Context.DELIMITER)
        if len(r) < 3:
            # TODO: 最小限制应为2
            # ts单独设列
            # 属性单独设列
            # DSL 变相支持
            continue
        try:
            h1, h2, ts = (
                chash(FROM_COL_HASH, r[0]),
                chash(TO_COL_HASH, r[1]),
                int(r[2]),
            )
        except ValueError:
            continue
        # 记录高频节点, 自动双向
        # 这里存储时每条边会存两次， 进入from_node_freq_lists/from_node_list 与to_node_freq_dict/to_node_lists
        if ffdict_able_flag and (h1 in from_node_freq_dict_set):
            from_node_freq_dict[h1].append((h2, ts))
        else:  # 记录非高频节点
            # from_node_lists[h1 // 0xf % FROM_SHORT_HASH].append((h1, h2))
            # try:
            from_node_lists[h1 & SHORT_HASH_MASK].append((h1, h2, ts))
            # except OverflowError:
            #     raise Exception("ERROR:", FROM_COL_HASH, r[0], TO_COL_HASH, r[1], h1, h2, ts)
            # print("ERROR:", h1, h2, ts)

        if tfdict_able_flag and (h2 in to_node_freq_dict_set):
            to_node_freq_dict[h2].append((h1, ts))
        else:
            # to_node_lists[h2 // 0xf % TO_SHORT_HASH].append((h2, h1))
            to_node_lists[h2 & SHORT_HASH_MASK].append((h2, h1, ts))

    # 将arraylist数据flush到磁盘中
    for arraylist in from_node_lists + to_node_lists:
        arraylist.close(merge=False)
    if ffdict_able_flag:
        for arraylist in from_node_freq_dict.values():
            arraylist.close(merge=False)
    if tfdict_able_flag:
        for arraylist in to_node_freq_dict.values():
            arraylist.close(merge=False)
    return True


def relationship2indexarray(dataset, graph, CHUNK_COUNT=cpu_count()):
    lines2idxarr_args = prepare_relations(dataset, graph, CHUNK_COUNT)
    dag = dask.delayed(sum)([lines2idxarr(*arg) for arg in lines2idxarr_args])
    dag.visualize(filename="dag.svg")
    return dag.compute()


# @dask.delayed
def prepare_relations(dataset, graph, CHUNK_COUNT=cpu_count()):
    key_sample_lines, nodes_line_num = lines_sampler(
        glob.glob(f"{dataset}/relation_*.csv")
    )
    freq_nodes, NODES_SHORT_HASH = node_hash_space_stat(
        key_sample_lines, nodes_line_num, graph
    )
    # return glob.glob(f"{dataset}/relation_*.csv")
    # with Pool(processes=CHUNK_COUNT) as pool:
    args = []
    for relation in glob.glob(f"{dataset}/relation_*.csv"):
        relation, basename = os.path.abspath(relation), os.path.basename(
            relation
        )
        # start_time = time.time()
        print("## Relationship to index array transforming... ##", relation)
        args += [
            (
                f"{graph}/{basename}.idxarr",
                splitfile_arguments,
                chunk_id,
                freq_nodes,
                NODES_SHORT_HASH,
                graph,
            )
            for chunk_id, splitfile_arguments in enumerate(
                SplitFile.split(relation, num=CHUNK_COUNT, jump=1)
            )
        ]

    return args
    #     # pool.starmap(
    #     #     lines2idxarr,
    #     #     [
    #     #         (
    #     #             f"{graph}/{basename}.idxarr",
    #     #             splitfile_arguments,
    #     #             chunk_id,
    #     #             freq_nodes,
    #     #             NODES_SHORT_HASH,
    #     #         )
    #     #         for chunk_id, splitfile_arguments in enumerate(
    #     #             SplitFile.split(relation, num=CHUNK_COUNT, jump=1)
    #     #         )
    #     #     ],
    #     # )
    #     print("Time usage:", time.time() - start_time)

    # return res


# relationship2indexarray and its helper functions
# ===================================Dividing line====================================================
# merge_index_array_then_sort and its helper functions


def merge_index_array_then_sort(graph):
    # chunk split
    chunk_files = list(glob.glob(f"{graph}/*.idxarr/hid_*_*.idxarr.chunk*"))
    freq_files = list(glob.glob(f"{graph}/*.idxarr/freq_edges/hid_*.chunk*"))
    files_hash_dict = defaultdict(list)
    files_freq_dict = defaultdict(list)
    for cfile in chunk_files:
        hash_id = re.findall(".*/(.+?).idxarr*", cfile)[0]
        files_hash_dict[hash_id].append(cfile)
    for ffile in freq_files:
        fname = os.path.basename(ffile)
        fid = int(fname.split("_")[1])
        files_freq_dict[fid].append(ffile)

    edges_count_sum = sum(
        [
            np.memmap(
                f,
                mode="r",
                dtype=[("from", np.int64), ("to", np.int64), ("ts", np.int32)],
            ).shape[0]
            for f in sum(list(files_hash_dict.values()), [])
        ]
    )
    edges_count_sum += sum(
        [
            np.memmap(
                f, mode="r", dtype=[("to", np.int64), ("ts", np.int32)]
            ).shape[0]
            for f in sum(list(files_freq_dict.values()), [])
        ]
    )

    os.makedirs(f"{graph}/edges_sort", exist_ok=True)
    MergeIndex.edge_to_cursor = 0
    print(f"{graph}/concat.to.arr", edges_count_sum)
    MergeIndex.toarrconcat = np.memmap(
        f"{graph}/concat.to.arr",
        mode="w+",
        shape=(edges_count_sum,),
        order="F",
        dtype=[("index", np.int64), ("value", np.int64), ("ts", np.int32)],
    )

    with Pool(processes=cpu_count()) as pool:
        stime = time.time()
        for items in list(files_hash_dict.items()):
            pool.apply_async(
                MergeIndex.merge_idx_to,
                args=items,
                callback=MergeIndex.merge_idx_to_callback,
            )
        for items in list(files_freq_dict.items()):
            pool.apply_async(
                MergeIndex.merge_freq_idx_to,
                args=items,
                callback=MergeIndex.merge_freq_idx_to_callback,
            )
        pool.close()
        pool.join()
        MergeIndex.freq_idx_pointer_dump(graph)
        print(time.time() - stime)


# merge_index_array_then_sort and its helper functions
# ===================================Dividing line====================================================
# hid_idx_merge
# After this, we have an dict pointing to an array, dict storing nodes' relationships' starting cursor and length,
# and array storing node's relationships
# Files other than edges sort and edges mapper can be removed at this step, probably.


def hid_idx_dict(graph, _id):
    # 所有short hid为_id的节点(不区分节点类型)索引全部都放入同一个字典中
    idxarr = [
        np.memmap(
            f,
            mode="r",
            dtype=[
                ("value", np.int64),
                ("index", np.int64),
                ("length", np.int32),
            ],
        )
        for f in glob.glob(f"{graph}/edges_sort/hid_%d_*.idx.arr" % _id)
    ]

    edges_index_count_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/edges_mapper", exist_ok=True)
    adict = ArrayDict(
        memmap_path=f"{graph}/edges_mapper/hid_%d.dict.arr" % _id,
        memmap=True,
        item_size=edges_index_count_sum,
        hash_heap_rate=0.5,
        value_dtype=[("index", np.int64), ("length", np.int32)],
        memmap_mode="w+",
    )

    for _, ia in enumerate(idxarr):
        # Note here how we put multiple values into ArrayDict
        # It's adict[index column] = adict[value columns]
        # It's not np.asarray[[ia['index'],ia['length']] since while accessing separately and combining gives
        # [[First row],[Second row]] while accessing with combined indices gives a list of
        # tuples: [(item1 in first column, item1 in second column),...], which is what we want
        adict[ia["value"]] = ia[["index", "length"]]
    # freq部分实际上是被重复写入到全部 hash short_dict中了
    hid_freq_path = f"{graph}/edges_sort/hid_freq.idx.arr"
    if os.path.exists(hid_freq_path):
        freqarr = np.memmap(
            hid_freq_path,
            mode="r",
            dtype=[
                ("value", np.int64),
                ("index", np.int64),
                ("length", np.int32),
            ],
        )
        adict[freqarr["value"]] = freqarr[["index", "length"]]


def hid_idx_merge(graph):
    with Pool(processes=cpu_count()) as p:
        stime = time.time()
        # TODO: BUG: 当样本过小时, hid连64个可能都凑不齐
        _ = p.starmap(hid_idx_dict, [(graph, i) for i in range(64)])
        print(time.time() - stime)


# hid_idx_merge
# ===================================Dividing line====================================================
# node2indexarray


def node2idxarr(output, splitfile_arguments, chunk_id):
    def random_chunk_size():
        return random.randint(300000, 600000)

    output = f"{output}"
    os.makedirs(output, exist_ok=True)
    with open(splitfile_arguments[0]) as f:
        line = f.readline()
        (NODE_COL,) = re.findall(r"\((.+?)\)", line)
        # TODO: NODE_FILE_HASH = Context.node_file_hash(splitfile_arguments[0])
        NODE_COL_HASH = Context.query_type_hash(NODE_COL)
    # 节点不再使用NODE_SHORT_HASH进行混合
    splitfile = SplitFile(*splitfile_arguments)
    data_type = [
        ("nid", np.int64),
        ("cursor", np.int64),
        ("chunk_id", np.int64),
        ("local_cursor", np.int64),
    ]
    data_type.extend(
        list(
            zip(
                Context.query_node_attr_name_without_str(NODE_COL),
                Context.query_node_attr_type_without_str(NODE_COL),
            )
        )
    )
    node_cursor_lists = ArrayList(
        "%s/hid_%s.curarr.chunk_%d" % (output, NODE_COL, chunk_id),
        # chunk_size=random_chunk_size(),
        chunk_size=random_chunk_size(),
        dtype=data_type,
    )
    _ = next(splitfile)
    cursor = splitfile.tell()
    # cursor = len(_l)
    # using the cursor in splitfile through the function tell is a much safer way to index the line info

    for line in splitfile:
        seg = line[:-1].split(Context.DELIMITER)
        nid = chash(NODE_COL_HASH, seg[0])
        seg = list(compress(seg[1:], Context.query_valid_attrs(NODE_COL)))
        attrs = [nid, cursor, chunk_id, len(node_cursor_lists)]
        attrs.extend(
            (
                list(
                    map(
                        Parse.get_value,
                        Context.query_node_attr_type_without_str(NODE_COL)[:],
                        seg[:],
                    )
                )
            )
        )
        node_cursor_lists.append(tuple(attrs))
        cursor = splitfile.tell()

    node_cursor_lists.close(merge=True)

    return chunk_id, len(node_cursor_lists)


def node2indexarray(dataset, graph, CHUNK_COUNT=cpu_count()):
    with Pool(processes=CHUNK_COUNT) as pool:
        for nodefile in glob.glob(f"{dataset}/node_*.csv"):
            nodefile, basename = os.path.abspath(nodefile), os.path.basename(
                nodefile
            )
            print("nodefile", nodefile, "basename", basename)
            node_type = basename.split("_")[1].split(".")[0]
            start_time = time.time()
            print(
                "## Node slicing and to index array transforming... ##",
                nodefile,
            )
            re_value = pool.starmap(
                node2idxarr,
                [
                    (
                        f"{graph}/{basename}.curarr",
                        splitfile_arguments,
                        chunk_id,
                    )
                    for chunk_id, splitfile_arguments in enumerate(
                        SplitFile.split(nodefile, num=CHUNK_COUNT, jump=1)
                    )
                ],
            )
            Context.node_attr_chunk_num[node_type] = dict(re_value)
            print(re_value)
            print("Time usage:", time.time() - start_time)

        print("Context.node_attr_chunk_num:", Context.node_attr_chunk_num)


# node2indexarray
# ===================================Dividing line====================================================
# merge_node_index


def merge_node_cursor_dict(graph, node_type):
    # 将每个含有节点信息的arraylist转为arraydict
    idxarr_directory = f"{graph}/node_{node_type}.csv.curarr"
    data_type = [
        ("nid", np.int64),
        ("cursor", np.int64),
        ("chunk_id", np.int64),
        ("local_cursor", np.int64),
    ]
    data_type.extend(
        list(
            zip(
                Context.query_node_attr_name_without_str(node_type),
                Context.query_node_attr_type_without_str(node_type),
            )
        )
    )
    idxarr = [
        np.memmap(f, mode="r", dtype=data_type)
        for f in glob.glob(f"{idxarr_directory}/hid_{node_type}.curarr.chunk_*")
    ]
    nodes_cursor_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/nodes_mapper", exist_ok=True)
    adict = ArrayDict(
        memmap_path=f"{graph}/nodes_mapper/{node_type}.dict.arr",
        memmap=True,
        item_size=nodes_cursor_sum,
        hash_heap_rate=0.5,
        value_dtype=[
            ("cursor", np.int64),
            ("chunk_id", np.int64),
            ("local_cursor", np.int64),
        ],
        memmap_mode="w+",
    )
    for chunk_id, ia in enumerate(idxarr):
        # using _id (as we previously did) would cause the _id in the local scope to change, not so safe
        adict[ia["nid"]] = ia[["cursor", "chunk_id", "local_cursor"]]


def merge_node_index(graph):
    print("### Merging node cursor to dict...")
    with Pool(processes=cpu_count()) as p:
        stime = time.time()
        p.starmap(
            merge_node_cursor_dict,
            [(graph, node_type) for node_type in Context.NODE_TYPE.keys()],
        )
        print(time.time() - stime)
        print("### Merge node cursor to dict complete")
    pass


# merge_node_index
# ===================================Dividing line====================================================
# load
def load_relationships(dataset, graph):
    print("Relationship Index Transforming...")
    relationship2indexarray(dataset, graph)
    print("Index resorted to edge mergeing...")
    merge_index_array_then_sort(graph)
    print("Graph index merge by hashid...")
    hid_idx_merge(graph)
    print("Merge success")


def load_nodes(dataset, graph):
    print("Node to hashid transforming...")
    node2indexarray(dataset, graph)
    print("Node index mapping...")
    merge_node_index(graph)


def load(dataset, graph):
    # Use fork instead of spawn to start processes so that all resources are inherited.
    # "Available on Unix only. The default on Unix."  -- quote https://docs.python.org/3/library/multiprocessing.html
    # multiprocessing.set_start_method("fork")

    # print with no omission
    np.set_printoptions(threshold=np.inf)

    print(f"Dataset: {dataset}\nGraph: {graph}")

    stime = time.time()
    print("T1:", stime)

    Context.load_loc(dataset, graph)
    # Prepare relations
    Context.load_node_type_hash(f"{graph}/node_type_id.json")
    Context.load_node_file_hash(f"{graph}/node_file_id.json")
    Context.prepare_relations(glob.glob(f"{dataset}/relation_*.csv"))
    Context.prepare_nodes(glob.glob(f"{dataset}/node_*.csv"))

    Context.close()

    print("Node attr name:\n", Context.node_attr_name)
    print("Node attr type:\n", Context.node_attr_type)
    print("Edge attr name:\n", Context.edge_attr_name)
    print("Edge attr type:\n", Context.edge_attr_type)

    # Process relationships
    load_relationships(dataset, graph)

    # Process nodes
    load_nodes(dataset, graph)

    etime = time.time()
    print("Load complete")
    print("T2:", etime)
    print("DT:", etime - stime)
