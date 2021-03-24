import glob
import numpy as np
import time
import os
import re
from collections import defaultdict, Counter
from multiprocessing import Pool, cpu_count
import random

from npgraph.tools.splitfile import SplitFile
from npgraph.tools.arraylist import ArrayList
from npgraph.tools.arraydict import ArrayDict
from npgraph.tools.hash import chash
from npgraph.context import Context
from npgraph.mergeindex import MergeIndex


def lines_sampler(relationship_files):
    """ 对图数据集做采样统计, 供后续估计hash空间使用, 提升导入效率
    """
    key_sample_lines, nodes_line_num = defaultdict(list), defaultdict(int)
    for relation in relationship_files:
        relation, basename = os.path.abspath(relation), os.path.basename(relation)
        with open(relation) as f:
            l = f.readline()
            from_col, to_col = re.findall("\((.+?)\)", l)
        splitfiles = SplitFile.split(relation, num=200, jump=1)
        filesize = os.path.getsize(relation)

        def sample_lines(sfs, cnt=1000):
            for arg in sfs:
                sf, i = SplitFile(*arg), 0
                for l in sf:
                    if i > cnt:
                        break
                    yield l
                    i += 1

        lines = list(sample_lines(splitfiles))
        for l in lines:
            keys = l.replace("\n", "").split(",")
            if len(keys) != 3:
                continue
            key_sample_lines[from_col].append(keys[0])
            key_sample_lines[to_col].append(keys[1])
        line_len_mean = np.mean(list(map(lambda l: len(l.encode('utf-8')), lines)))
        line_num = int(filesize / line_len_mean)
        nodes_line_num[from_col] += line_num
        nodes_line_num[to_col] += line_num
    return key_sample_lines, nodes_line_num


def node_hash_space_stat(key_sample_lines, nodes_line_num,
                         without_freq=False,
                         HID_BATCH_SIZE_AVERAGE=int(5e6),
                         FREQ_WORDS_TOP_KEEP=10,
                         FREQ_WORDS_TOP_RATIO=0.01):
    """ 根据数据集的分布情况自动切分hash空间和高频节点列表
    """
    NODES_SHORT_HASH, freq_nodes = {}, {}
    for node, l in nodes_line_num.items():
        NODES_SHORT_HASH[node] = l // HID_BATCH_SIZE_AVERAGE
    if without_freq:
        return freq_nodes, NODES_SHORT_HASH

    for node, words in key_sample_lines.items():
        node_freq_word_cnt = Counter(words).most_common(FREQ_WORDS_TOP_KEEP)
        csum = len(words)
        topstat = [(node, fcnt, fcnt / csum) for node, fcnt in node_freq_word_cnt]
        freqitem = list(filter(lambda x: x[2] > FREQ_WORDS_TOP_RATIO, topstat))
        freqrate = sum([e[-1] for e in freqitem])
        # HASH空间切分数 = 非高频节点的记录数 / 期望的hash batch大小
        NODES_SHORT_HASH[node] = int(nodes_line_num[node] * (1 - freqrate)) // HID_BATCH_SIZE_AVERAGE
        # 建立节点高频word集合
        if len(freqitem) > 0:
            freq_nodes[node] = set([chash(Context.node_type_hash(node), item[0]) for item in freqitem])
    return freq_nodes, NODES_SHORT_HASH


def lines2idxarr(output, splitfile_arguments, chunk_id, freq_nodes, NODES_SHORT_HASH):
    # output, (path, _from, _to), chunk = args
    with open(splitfile_arguments[0]) as f:
        FROM_COL, TO_COL = re.findall("\((.+?)\)", f.readline())
        FROM_COL_HASH, TO_COL_HASH = Context.node_type_hash(FROM_COL), Context.node_type_hash(TO_COL)
    # FROM_SHORT_HASH, TO_SHORT_HASH = NODES_SHORT_HASH[FROM_COL], NODES_SHORT_HASH[TO_COL]
    # 固定为64的原因主要还是考虑后续会映射到edge dict中, 统一使用64bin去切割
    FROM_SHORT_HASH, TO_SHORT_HASH, SHORT_HASH_MASK = 64, 64, (1 << 6) - 1

    splitfile = SplitFile(*splitfile_arguments)
    os.makedirs(output, exist_ok=True)
    # 随机块大小是为了让进程吃资源的节奏错开
    random_chunk_size = lambda: random.randint(300000, 600000)
    # 针对非高频节点使用使用短hash映射到共享空间，需要独立重排
    from_node_lists = [ArrayList("%s/hid_%d_%s.idxarr.chunk_%d" % \
                                 (output, i, FROM_COL, chunk_id),
                                 chunk_size=random_chunk_size(),
                                 dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)])
                       for i in range(FROM_SHORT_HASH)]
    to_node_lists = [ArrayList("%s/hid_%d_%s.idxarr.chunk_%d" % \
                               (output, i, TO_COL, chunk_id),
                               chunk_size=random_chunk_size(),
                               dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)])
                     for i in range(TO_SHORT_HASH)]
    # 针对高频节点， 使用独立的空间， 不需要重排
    freq_output = f"{output}/freq_edges"
    os.makedirs(freq_output, exist_ok=True)
    ffdict_able_flag, tfdict_able_flag = False, False
    if FROM_COL in freq_nodes:
        from_node_freq_dict = {fk: ArrayList("%s/hid_%s_%s.idxarr.chunk_%d" % \
                                             (freq_output, str(fk), FROM_COL, chunk_id),
                                             chunk_size=random_chunk_size(),
                                             dtype=[('to', np.int64), ('ts', np.int32)])
                               #   dtype=[('from', np.int64), ('to', np.int64)])
                               for fk in list(freq_nodes[FROM_COL])}
        from_node_freq_dict_set = set(from_node_freq_dict.keys())
        ffdict_able_flag = True
    if TO_COL in freq_nodes:
        to_node_freq_dict = {tk: ArrayList("%s/hid_%s_%s.idxarr.chunk_%d" % \
                                           (freq_output, str(tk), TO_COL, chunk_id),
                                           chunk_size=random_chunk_size(),
                                           dtype=[('to', np.int64), ('ts', np.int32)])
                             #   dtype=[('from', np.int64), ('to', np.int64)])
                             for tk in list(freq_nodes[TO_COL])}
        to_node_freq_dict_set = set(to_node_freq_dict.keys())
        tfdict_able_flag = True

    for line in splitfile:
        # from_str, to_str
        r = line[:-1].split(",")
        if len(r) != 3:
            continue
        try:
            h1, h2, ts = chash(FROM_COL_HASH, r[0]), chash(TO_COL_HASH, r[1]), int(r[2])
        except ValueError:
            continue
        # 记录高频节点, 自动双向
        if ffdict_able_flag and (h1 in from_node_freq_dict_set):
            from_node_freq_dict[h1].append((h2, ts))
        else:  # 记录非高频节点
            # from_node_lists[h1 // 0xf % FROM_SHORT_HASH].append((h1, h2))
            from_node_lists[h1 & SHORT_HASH_MASK].append((h1, h2, ts))

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


def relationship2indexarray(dataset, graph, CHUNK_COUNT=cpu_count()):
    key_sample_lines, nodes_line_num = lines_sampler(glob.glob(f"{dataset}/relation_*.csv"))
    freq_nodes, NODES_SHORT_HASH = node_hash_space_stat(key_sample_lines, nodes_line_num)
    pool = Pool(processes=CHUNK_COUNT)
    for relation in glob.glob(f"{dataset}/relation_*.csv"):
        relation, basename = os.path.abspath(relation), os.path.basename(relation)
        start_time = time.time()
        print("## Relationship to index array transforming... ##", relation)
        pool.starmap(lines2idxarr,
                     [(f"{graph}/{basename}.idxarr",
                       splitfile_arguments,
                       chunk_id,
                       freq_nodes,
                       NODES_SHORT_HASH)
                      for chunk_id, splitfile_arguments in
                      enumerate(SplitFile.split(relation, num=CHUNK_COUNT, jump=1))])
        print("Time usage:", time.time() - start_time)


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
        fid = int(fname.split('_')[1])
        files_freq_dict[fid].append(ffile)

    edges_count_sum = sum(
        [np.memmap(f, mode='r', dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)]).shape[0]
         for f in sum(list(files_hash_dict.values()), [])])
    edges_count_sum += sum([np.memmap(f, mode='r', dtype=[('to', np.int64), ('ts', np.int32)]).shape[0]
                            for f in sum(list(files_freq_dict.values()), [])])

    os.makedirs(f"{graph}/edges_sort", exist_ok=True)
    MergeIndex.edge_to_cursor = 0
    MergeIndex.toarrconcat = np.memmap(f"{graph}/concat.to.arr",
                                       mode='w+',
                                       shape=(edges_count_sum,),
                                       order='F',
                                       dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])

    pool = Pool(processes=cpu_count())
    stime = time.time()
    for items in list(files_hash_dict.items()):
        pool.apply_async(MergeIndex.merge_idx_to,
                         args=items,
                         callback=MergeIndex.merge_idx_to_callback)
    for items in list(files_freq_dict.items()):
        pool.apply_async(MergeIndex.merge_freq_idx_to,
                         args=items,
                         callback=MergeIndex.merge_freq_idx_to_callback)
    pool.close()
    pool.join()
    MergeIndex.freq_idx_pointer_dump(graph)
    print(time.time() - stime)


# merge_index_array_then_sort and its helper functions
# ===================================Dividing line====================================================
# hid_idx_merge

def hid_idx_dict(graph, _id):
    # 所有short hid为_id的节点(不区分节点类型)索引全部都放入同一个字典中
    idxarr = [np.memmap(f,
                        mode='r',
                        dtype=[('value', np.int64),
                               ('index', np.int64),
                               ('length', np.int32)])
              for f in glob.glob(f"{graph}/edges_sort/hid_%d_*.idx.arr" % _id)]
    freqarr = np.memmap(f"{graph}/edges_sort/hid_freq.idx.arr",
                        mode='r',
                        dtype=[('value', np.int64),
                               ('index', np.int64),
                               ('length', np.int32)])
    edges_index_count_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/edges_mapper", exist_ok=True)
    adict = ArrayDict(memmap_path=f"{graph}/edges_mapper/hid_%d.dict.arr" % _id,
                      memmap=True,
                      item_size=edges_index_count_sum,
                      hash_heap_rate=0.5,
                      value_dtype=[('index', np.int64), ('length', np.int32)],
                      memmap_mode='w+')

    for _id, ia in enumerate(idxarr):
        adict[ia['value']] = ia[['index', 'length']]
    # freq部分实际上是被重复写入到全部 hash short_dict中了
    adict[freqarr['value']] = freqarr[['index', 'length']]


def hid_idx_merge(graph):
    p = Pool(processes=cpu_count())
    stime = time.time()
    res = p.starmap(hid_idx_dict, [(graph, i) for i in range(64)])
    print(time.time() - stime)


# hid_idx_merge
# ===================================Dividing line====================================================
# node2indexarray

def node2idxarr(output, splitfile_arguments, chunk_id):
    output = f"{output}"
    os.makedirs(output, exist_ok=True)
    with open(splitfile_arguments[0]) as f:
        l = f.readline()
        NODE_COL, = re.findall("\((.+?)\)", l)
        NODE_FILE_HASH = Context.node_file_hash(splitfile_arguments[0])
        NODE_COL_HASH = Context.node_type_hash(NODE_COL)
    # FROM_SHORT_HASH, TO_SHORT_HASH = NODES_SHORT_HASH[FROM_COL], NODES_SHORT_HASH[TO_COL]
    # 固定为64的原因主要还是考虑后续会映射到edge dict中, 统一使用64bin去切割
    NODE_SHORT_HASH, SHORT_HASH_MASK = 64, (1 << 6) - 1
    random_chunk_size = lambda: random.randint(300000, 600000)
    splitfile = SplitFile(*splitfile_arguments)
    node_cursor_lists = [ArrayList("%s/hid_%d_%s.curarr.chunk_%d" % (output, i, NODE_COL, chunk_id),
                                   chunk_size=random_chunk_size(),
                                   dtype=[('nid', np.int64), ('cursor', np.int64), ('bool_data', np.bool),
                                          ('int_data', np.int), ('float_data', np.float)])
                         for i in range(NODE_SHORT_HASH)]
    _l = next(splitfile)
    cursor = len(_l)
    import ast

    def parse(string_list):
        re_list = []
        for string in string_list:
            try:
                re_list.append(ast.literal_eval(string))
            except:
                re_list.append(string)
        return re_list

    for l in splitfile:
        seg = l[:-1].split(",")
        seg = parse(seg)
        nid = chash(NODE_COL_HASH, seg[0])
        # boolean, int, float
        node_cursor_lists[nid & SHORT_HASH_MASK].append((nid, NODE_FILE_HASH | cursor, False, 100, 99.9))
        cursor += len(l)

    for arraylist in node_cursor_lists:
        arraylist.close(merge=False)
    pass


def node2indexarray(dataset, graph, CHUNK_COUNT=cpu_count()):
    pool = Pool(processes=CHUNK_COUNT)
    for nodefile in glob.glob(f"{dataset}/node_*.csv"):
        nodefile, basename = os.path.abspath(nodefile), os.path.basename(nodefile)
        start_time = time.time()
        print("## Node to index array transforming... ##", nodefile)
        pool.starmap(node2idxarr,
                     [(f"{graph}/{basename}.curarr",
                       splitfile_arguments,
                       chunk_id)
                      for chunk_id, splitfile_arguments in
                      enumerate(SplitFile.split(nodefile, num=CHUNK_COUNT, jump=1))]
                     )
        print("Time usage:", time.time() - start_time)


# node2indexarray
# ===================================Dividing line====================================================
# merge_node_index

def merge_node_cursor_dict(graph, _id):
    # 所有short hid为_id的节点(不区分节点类型)索引全部都放入同一个字典中
    # file seek has time complexity of O(1) if given the file pointer (here, cursor).
    idxarr = [np.memmap(f,
                        mode='r',
                        dtype=[('idx', np.int64),
                               ('cursor', np.int64),
                               ('bool_data', np.bool),
                               ('int_data', np.int),
                               ('float_data', np.float)])
              for f in glob.glob(f"{graph}/node_*.csv.curarr/hid_%d_*.curarr.chunk*" % _id)]
    nodes_cursor_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/nodes_mapper", exist_ok=True)
    adict = ArrayDict(memmap_path=f"{graph}/nodes_mapper/hid_%d.dict.arr" % _id,
                      memmap=True,
                      item_size=nodes_cursor_sum,
                      hash_heap_rate=0.5,
                      value_dtype=[('cursor', np.int64), ('bool_data', np.bool), ('int_data', np.int),
                                   ('float_data', np.float)],
                      memmap_mode='w+')

    for _id, ia in enumerate(idxarr):
        adict[ia['idx']]['cursor'] = ia['cursor']
        adict[ia['idx']]['bool_data'] = ia['bool_data']
        adict[ia['idx']]['int_data'] = ia['int_data']
        adict[ia['idx']]['float_data'] = ia['float_data']


def merge_node_index(graph):
    p = Pool(processes=cpu_count())
    stime = time.time()
    res = p.starmap(merge_node_cursor_dict, [(graph, i) for i in range(64)])
    print(time.time() - stime)
    pass
