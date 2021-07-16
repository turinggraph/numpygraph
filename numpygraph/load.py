import numpy as np
import os
import re
from collections import defaultdict, Counter
from multiprocessing import Pool, cpu_count
import random
from itertools import compress, chain

from numpygraph.lib.splitfile import SplitFile
from numpygraph.core.arraylist import ArrayList
from numpygraph.core.arraydict import ArrayDict
from numpygraph.core.hash import chash
from numpygraph.mergeindex import MergeIndex
from numpygraph.core.parse import Parse


def lines_sampler(relationship_files):
    """
    Sample relation files for the following split and dump steps.
    """
    # 对图数据集做采样统计, 供后续估计hash空间使用, 提升导入效率
    # relationship_files = context.relation_files
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
            keys = line.replace("\n", "").split(",")
            if len(keys) != 3:
                continue
            key_sample_lines[from_col].append(keys[0])
            key_sample_lines[to_col].append(keys[1])
        line_len_mean = np.mean(list(map(lambda l: len(l.encode('utf-8')), lines)))
        line_num = int(filesize / line_len_mean)
        nodes_line_num[from_col] += line_num
        nodes_line_num[to_col] += line_num
    return key_sample_lines, nodes_line_num


def node_hash_space_stat(context, key_sample_lines_AND_nodes_line_num,
                         without_freq=False,
                         HID_BATCH_SIZE_AVERAGE=int(5e6),
                         FREQ_WORDS_TOP_KEEP=10,
                         FREQ_WORDS_TOP_RATIO=0.01):
    """
    Find all freq nodes in the relation files and build an index
    """
    # 根据数据集的分布情况自动切分hash空间和高频节点列表
    key_sample_lines, nodes_line_num = key_sample_lines_AND_nodes_line_num
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
            freq_nodes[node] = set([chash(context.query_type_hash(node), item[0]) for item in freqitem])
    return freq_nodes, NODES_SHORT_HASH


def relationship2indexarray(context, node_hash_space_stat_result, relation_files, CHUNK_COUNT=cpu_count()):
    """
    Split relation files, passing the split parts to :func:`numpygraph.load.lines2idxarr` to utilize multiprocessing. This is a function distributing jobs.

    :type context: Context
    :param context: context object we always read from
    :param node_hash_space_stat_result: result from :func:`numpygraph.load.node_hash_space_stat`
    :type relation_files: list of str
    :param relation_files: list of relation files in the dataset folder (also available in `context`, but we need to expose all file dependencies to avoid unwanted concurrent read/write.
    :type CHUNK_COUNT: int
    :param CHUNK_COUNT: desired number of chunk that relation files to split into
    :return: a tuple of two lists: list of dump files for infrequent nodes and list of dump files for frequent nodes
    """
    freq_nodes, NODES_SHORT_HASH = node_hash_space_stat_result
    pool = Pool(processes=CHUNK_COUNT)
    normal_files = []
    freq_files = []
    for relation in relation_files:
        relation, basename = os.path.abspath(relation), os.path.basename(relation)
        # normal_file, freq_file =
        files = pool.starmap(lines2idxarr,
                             [(f"{context.graph}/{basename}.idxarr",
                               splitfile_arguments,
                               chunk_id,
                               freq_nodes,
                               NODES_SHORT_HASH,
                               context)
                              for chunk_id, splitfile_arguments in
                              enumerate(SplitFile.split(relation, num=CHUNK_COUNT, jump=1))])
        transposed = list(zip(*files))
        normal_files.extend(list(chain(*transposed[0])))
        freq_files.extend(list(chain(*transposed[1])))
        # normal_files.extend(normal_file)
        # freq_files.extend(freq_file)
    return normal_files, freq_files


def lines2idxarr(output, splitfile_arguments, chunk_id, freq_nodes, NODES_SHORT_HASH, context):
    """
    Processes jobs given out by :func:`numpygraph.load.relationship2indexarray`, reading slices of relationship files, classifying relations by node's frequency and hash values of relationship ids.

    :param output: output location of arraylists of node mapping to edges
    :param splitfile_arguments: file pointer to the splitfile
    :param chunk_id: the numbering of the portion of splitile in the original file
    :param freq_nodes: a list of freq nodes
    :param NODES_SHORT_HASH: module applied to nodes that mapping nodes' hash value to [0, NODES_SHORT_HASH)
    :param context: context
    :return: a list of file paths `[normal_files, freq_files]` of all files written to disk (still for solving file dependencies)
    """
    # writes to two types of files: freq arraylist and infrequent (normal) arraylist
    # output, (path, _from, _to), chunk = args
    freq_files = []
    normal_files = []
    with open(splitfile_arguments[0]) as f:
        FROM_COL, TO_COL = re.findall(r"\((.+?)\)", f.readline())
        FROM_COL_HASH, TO_COL_HASH = context.query_type_hash(FROM_COL), context.query_type_hash(TO_COL)
    # FROM_SHORT_HASH, TO_SHORT_HASH = NODES_SHORT_HASH[FROM_COL], NODES_SHORT_HASH[TO_COL]
    # 固定为64的原因主要还是考虑后续会映射到edge dict中, 统一使用64bin去切割
    FROM_SHORT_HASH, TO_SHORT_HASH, SHORT_HASH_MASK = 64, 64, (1 << 6) - 1

    splitfile = SplitFile(*splitfile_arguments)
    os.makedirs(output, exist_ok=True)

    # 随机块大小是为了让进程吃资源的节奏错开
    def random_chunk_size():
        return random.randint(300000, 600000)

    # 针对非高频节点使用使用短hash映射到共享空间，需要独立重排
    from_node_lists = [ArrayList(f"{output}/hid_{i}_{FROM_COL}.idxarr.chunk_{chunk_id}",
                                 chunk_size=random_chunk_size(),
                                 dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)])
                       for i in range(FROM_SHORT_HASH)]
    to_node_lists = [ArrayList(f"{output}/hid_{i}_{TO_COL}.idxarr.chunk_{chunk_id}",
                               chunk_size=random_chunk_size(),
                               dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)])
                     for i in range(TO_SHORT_HASH)]
    # normal_files.extend([f"{output}/hid_{i}_{FROM_COL}.idxarr.chunk_{chunk_id}" for i in range(FROM_SHORT_HASH)])
    # normal_files.extend([f"{output}/hid_{i}_{TO_COL}.idxarr.chunk_{chunk_id}" for i in range(TO_SHORT_HASH)])

    # 针对高频节点， 使用独立的空间， 不需要重排
    freq_output = f"{output}/freq_edges"
    os.makedirs(freq_output, exist_ok=True)
    ffdict_able_flag, tfdict_able_flag = False, False
    if FROM_COL in freq_nodes:
        from_node_freq_dict = {
            fk: ArrayList("%s/hid_%s_%s.idxarr.chunk_%d" % (freq_output, str(fk), FROM_COL, chunk_id),
                          chunk_size=random_chunk_size(),
                          dtype=[('to', np.int64), ('ts', np.int32)])
            #   dtype=[('from', np.int64), ('to', np.int64)])
            for fk in list(freq_nodes[FROM_COL])}
        from_node_freq_dict_set = set(from_node_freq_dict.keys())
        ffdict_able_flag = True
        # freq_files.extend(["%s/hid_%s_%s.idxarr.chunk_%d" % (freq_output, str(fk), FROM_COL, chunk_id) for fk in list(freq_nodes[FROM_COL])])

    if TO_COL in freq_nodes:
        to_node_freq_dict = {
            tk: ArrayList("%s/hid_%s_%s.idxarr.chunk_%d" % (freq_output, str(tk), TO_COL, chunk_id),
                          chunk_size=random_chunk_size(),
                          dtype=[('to', np.int64), ('ts', np.int32)])
            #   dtype=[('from', np.int64), ('to', np.int64)])
            for tk in list(freq_nodes[TO_COL])}
        to_node_freq_dict_set = set(to_node_freq_dict.keys())
        tfdict_able_flag = True
        # freq_files.extend(["%s/hid_%s_%s.idxarr.chunk_%d" % (freq_output, str(tk), TO_COL, chunk_id) for tk infreq_nodes[TO_COL])])

    for line in splitfile:
        # from_str, to_str
        r = line[:-1].split(",")
        if len(r) < 3:
            # TODO: 最小限制应为2
            # ts单独设列
            # 属性单独设列
            # DSL 变相支持
            continue
        try:
            h1, h2, ts = chash(FROM_COL_HASH, r[0]), chash(TO_COL_HASH, r[1]), int(r[2])
        except ValueError:
            continue
        # 记录高频节点, 自动双向
        # 这里存储时每条边会存两次， 进入from_node_freq_lists/from_node_list 与to_node_freq_dict/to_node_lists
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
        normal_files.extend(arraylist.close(merge=False))
    if ffdict_able_flag:
        for arraylist in from_node_freq_dict.values():
            freq_files.extend(arraylist.close(merge=False))
    if tfdict_able_flag:
        for arraylist in to_node_freq_dict.values():
            freq_files.extend(arraylist.close(merge=False))
    # print("normal_files", normal_files)
    # print("freq_files", freq_files)
    return [normal_files, freq_files]


# relationship2indexarray and its helper functions
# ===================================Dividing line====================================================
# merge_index_array_then_sort and its helper functions
def edge_count_sum_chunk(chunk_files):
    """
    Take relation files with normal nodes, returns a dict of these files
    """
    # chunk_files = list(glob.glob(f"{context.graph}/*.idxarr/hid_*_*.idxarr.chunk*"))
    files_hash_dict = defaultdict(list)
    for cfile in chunk_files:
        hash_id = re.findall(".*/(.+?).idxarr*", cfile)[0]
        files_hash_dict[hash_id].append(cfile)
    return files_hash_dict


def edge_count_sum_freq(freq_files):
    """
    Takes relation files with freq nodes, returns a dict of these files
    """
    # freq_files = list(glob.glob(f"{context.graph}/*.idxarr/freq_edges/hid_*.chunk*"))
    files_freq_dict = defaultdict(list)
    for ffile in freq_files:
        fname = os.path.basename(ffile)
        fid = int(fname.split('_')[1])
        files_freq_dict[fid].append(ffile)
    return files_freq_dict


def summing(files_hash_dict, files_freq_dict):
    """
    Take a dict to all nodes, return number of edges indexed by the mapping from node ids to edges.
    """
    result = sum(
        [np.memmap(f, mode='r', dtype=[('from', np.int64), ('to', np.int64), ('ts', np.int32)]).shape[0]
         for f in sum(list(files_hash_dict.values()), [])])
    result += sum([np.memmap(f, mode='r', dtype=[('to', np.int64), ('ts', np.int32)]).shape[0]
                   for f in sum(list(files_freq_dict.values()), [])])
    return result


def MergeIndex_gen(context, edges_count_sum):
    """
    Generate the MergeIndex object (also a context) used in the next Task
    """
    os.makedirs(f"{context.graph}/edges_sort", exist_ok=True)
    mergeindex = MergeIndex()
    mergeindex.edge_to_cursor = 0
    print("edges_count_sum =", edges_count_sum)
    mergeindex.toarrconcat = np.memmap(f"{context.graph}/concat.to.arr",
                                       mode='w+',
                                       shape=(edges_count_sum,),
                                       order='F',
                                       dtype=[('index', np.int64), ('value', np.int64), ('ts', np.int32)])
    return mergeindex


def merge_freq_and_other_idx_to(context, files_hash_dict, files_freq_dict, mergeindex):
    """
    Put all mapping from node hash to edge into mergeindex.toarrconcat, an numpy.memmap object.
    Uses `pool.apply_async`, note that callback function is called here in the context of `merge_freq_and_other_idx_to` function, not in the context of function `mergeindex.merge_idx_to` or `mergeindex.merge_freq_idx_to` so that things similar to MergeIndex.freq_idx_pointer can be synced in each call and callback function.
    """
    pool = Pool(processes=cpu_count())
    for items in list(files_hash_dict.items()):
        pool.apply_async(mergeindex.merge_idx_to, args=items, callback=mergeindex.merge_idx_to_callback)

    for items in list(files_freq_dict.items()):
        pool.apply_async(mergeindex.merge_freq_idx_to, args=items, callback=mergeindex.merge_freq_idx_to_callback)
    pool.close()
    pool.join()

    # print(mergeindex.filenames)

    # edges_sort folder's contents is written here
    freq_idx_pointer_dump_path = mergeindex.freq_idx_pointer_dump(context)
    return mergeindex.filenames, freq_idx_pointer_dump_path


# merge_index_array_then_sort and its helper functions
# ===================================Dividing line====================================================
# hid_idx_merge
# After this, we have an dict pointing to an array, dict storing nodes' relationships' starting cursor and length,
# and array storing node's relationships
# Files other than edges sort and edges mapper can be removed at this step, probably.

def hid_idx_dict(graph, _id, edge_mapper_files, hid_freq_path):
    """
    Categorize nodes with short id value, put all those with short id being `_id` to an ArrayDict

    :return: ArrayDict written to disk
    """
    # 所有short hid为_id的节点(不区分节点类型)索引全部都放入同一个字典中
    # needs edges_sort/hid_*.idx.arr, edges_mapper/hid_*.dic.arr
    # edges_sort / hid_freq.idx.arr
    # print("edge_mapper_files:", edge_mapper_files)
    idxarr = [np.memmap(f,
                        mode='r',
                        dtype=[('value', np.int64),
                               ('index', np.int64),
                               ('length', np.int32)])
              for f in edge_mapper_files]

    edges_index_count_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/edges_mapper", exist_ok=True)
    adict = ArrayDict(memmap_path=f"{graph}/edges_mapper/hid_%d.dict.arr" % _id,
                      memmap=True,
                      item_size=edges_index_count_sum,
                      hash_heap_rate=0.5,
                      value_dtype=[('index', np.int64), ('length', np.int32)],
                      memmap_mode='w+')

    for _, ia in enumerate(idxarr):
        # Note here how we put multiple values into ArrayDict
        # It's adict[index column] = adict[value columns]
        # It's not np.asarray[[ia['index'],ia['length']] since while accessing separately and combining gives
        # [[First row],[Second row]] while accessing with combined indices gives a list of
        # tuples: [(item1 in first column, item1 in second column),...], which is what we want
        adict[ia['value']] = ia[['index', 'length']]
    # freq部分实际上是被重复写入到全部 hash short_dict中了
    if hid_freq_path is not None:
        freqarr = np.memmap(hid_freq_path,
                            mode='r',
                            dtype=[('value', np.int64),
                                   ('index', np.int64),
                                   ('length', np.int32)])
        adict[freqarr['value']] = freqarr[['index', 'length']]
    return f"{graph}/edges_mapper/hid_{_id}.dict.arr"


def hid_idx_merge(context, FILES_edge_mapper, hid_freq_path):
    """
    Put mapping from node hash to edge with the same node short hash to a single file, returning a list of files of different node short hash id.
    """
    p = Pool(processes=cpu_count())
    graph = context.graph
    # print("FILES_edge_mapper:", FILES_edge_mapper)
    files = p.starmap(hid_idx_dict, [(graph, i, FILES_edge_mapper[i], hid_freq_path) for i in range(64)])
    return files


# hid_idx_merge
# ===================================Dividing line====================================================
# node2indexarray

def node2idxarr(output, splitfile_arguments, chunk_id, context):
    """
    Process each splitfile, categorizing nodes based on their hash id.
    """

    def random_chunk_size():
        return random.randint(300000, 600000)

    output = f"{output}"
    os.makedirs(output, exist_ok=True)
    with open(splitfile_arguments[0]) as f:
        line = f.readline()
        NODE_COL, = re.findall(r"\((.+?)\)", line)
        # TODO: NODE_FILE_HASH = Context.node_file_hash(splitfile_arguments[0])
        NODE_COL_HASH = context.query_type_hash(NODE_COL)
    # 节点不再使用NODE_SHORT_HASH进行混合
    splitfile = SplitFile(*splitfile_arguments)
    data_type = [('nid', np.int64), ('cursor', np.int64), ('chunk_id', np.int64), ('local_cursor', np.int64)]
    data_type.extend(
        list(zip(context.query_node_attr_name_without_str(NODE_COL),
                 context.query_node_attr_type_without_str(NODE_COL)))
    )
    node_cursor_lists = ArrayList("%s/hid_%s.curarr.chunk_%d" % (output, NODE_COL, chunk_id),
                                  chunk_size=random_chunk_size(),
                                  dtype=data_type)
    _ = next(splitfile)
    cursor = splitfile.tell()
    # cursor = len(_l)
    # using the cursor in splitfile through the function tell is a much safer way to index the line info

    for line in splitfile:
        seg = line[:-1].split(",")
        nid = chash(NODE_COL_HASH, seg[0])
        seg = list(compress(seg[1:], context.query_valid_attrs(NODE_COL)))
        attrs = [nid, cursor, chunk_id, len(node_cursor_lists)]
        # cannot convert dictionary update sequence element #0 to a sequence
        attrs.extend((list(map(Parse.get_value, context.query_node_attr_type_without_str(NODE_COL)[:], seg[:]))))
        node_cursor_lists.append(tuple(attrs))
        cursor = splitfile.tell()

    node_cursor_lists.close(merge=True)

    return (chunk_id, len(node_cursor_lists)), f"{output}/hid_{NODE_COL}.curarr.chunk_{chunk_id}"


def node2indexarray(context, node_files, CHUNK_COUNT=cpu_count()):
    """
    Split node files and pass seperated split files to :func:`numpygraph.load.node2inxarr`, eventually categroze nodes based on their hash id.
    """
    pool = Pool(processes=CHUNK_COUNT)
    written_files = defaultdict(list)
    for nodefile in node_files:
        nodefile, basename = os.path.abspath(nodefile), os.path.basename(nodefile)
        node_type = basename.split('_')[1].split('.')[0]
        # re_value, written_file
        mixed = pool.starmap(node2idxarr,
                             [(f"{context.graph}/{basename}.curarr",
                               splitfile_arguments,
                               chunk_id,
                               context)
                              for chunk_id, splitfile_arguments in
                              enumerate(SplitFile.split(nodefile, num=CHUNK_COUNT, jump=1))]
                             )
        unzipped = zip(*mixed)
        re_value = list(unzipped)[0]
        unzipped = zip(*mixed)
        written_file = list(unzipped)[1]
        context.node_attr_chunk_num[node_type] = dict(re_value)
        written_files[node_type].extend(written_file)

    return written_files


# node2indexarray
# ===================================Dividing line====================================================
# merge_node_index

# merge node index

def merge_node_cursor_dict(context, node_type, node_files):
    """
    Convert each ArrayList of nodes of a specific node type information to ArrayDict.
    """
    # 将每个含有节点信息的arraylist转为arraydict
    graph = context.graph
    data_type = [('nid', np.int64), ('cursor', np.int64), ('chunk_id', np.int64), ('local_cursor', np.int64)]
    data_type.extend(
        list(zip(context.query_node_attr_name_without_str(node_type),
                 context.query_node_attr_type_without_str(node_type)))
    )
    idxarr = [
        np.memmap(f,
                  mode='r',
                  dtype=data_type
                  )
        for f in node_files
    ]
    nodes_cursor_sum = sum([i.shape[0] for i in idxarr])
    os.makedirs(f"{graph}/nodes_mapper", exist_ok=True)
    adict = ArrayDict(memmap_path=f"{graph}/nodes_mapper/{node_type}.dict.arr",
                      memmap=True,
                      item_size=nodes_cursor_sum,
                      hash_heap_rate=0.5,
                      value_dtype=[('cursor', np.int64), ('chunk_id', np.int64), ('local_cursor', np.int64)],
                      memmap_mode='w+')
    for chunk_id, ia in enumerate(idxarr):
        # using _id (as we previously did) would cause the _id in the local scope to change, not so safe
        adict[ia['nid']] = ia[['cursor', 'chunk_id', 'local_cursor']]

    return f"{graph}/nodes_mapper/{node_type}.dict.arr"


def merge_node_index(context, node_curarr_files):
    """
    Convert all ArrayLists of node info to ArrayDict.
    """
    p = Pool(processes=cpu_count())
    nodes_mapper_files = p.starmap(merge_node_cursor_dict,
                                   [(context, node_type, node_curarr_files[node_type]) for node_type in
                                    context.NODE_TYPE.keys()])
    return nodes_mapper_files

# merge_node_index
