import time

from tinydag.tinydag import Task, EndTask, DAG, Logger
from tinydag.tinydag import MultiProcessTask

import numpygraph
import numpygraph.load
from numpygraph.lib.splitfile import SplitFile
from numpygraph.core.arraylist import ArrayList
from numpygraph.core.arraydict import ArrayDict
from numpygraph.core.hash import chash
from numpygraph.context import Context
from numpygraph.mergeindex import MergeIndex
from numpygraph.core.parse import Parse

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import cpu_count

import os
import glob
import random

logger = Logger()


def dag_load(dataset_path, graph_path):
    thread_num = 32
    print(f"Dataset: {dataset_path}\nGraph: {graph_path}")
    # load context
    context = Context().load_context(dataset_path, graph_path)
    # print(context)

    processpool = ProcessPoolExecutor()

    def end(*args, **kwargs):
        print("ENDING...")
        print("*args:", *args)
        print("**kwargs", **kwargs)
        return 0

    # load relationships
    relationship2indexarray = DAG(
        {
            "lines_sampler": Task(numpygraph.load.lines_sampler, "$context"),
            "node_hash_space_stat": Task(numpygraph.load.node_hash_space_stat, "$context", "$lines_sampler"),
            # new function in numpygraph/load.py
            "lines2idxarr_arg_gen": Task(numpygraph.load.linesidxarr_arg_gen, "$context", "$node_hash_space_stat"),
            # function at line 105, originally invoked from around line 170
            "lines2idxarr": MultiProcessTask(processpool, numpygraph.load.lines2idxarr, "$lines2idxarr_arg_gen"),
            "End": EndTask(end, "$lines2idxarr"),
        }
    )(context=context)

    # load nodes
    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        relationship2indexarray.execute(pool).get()

    # merge index array then sort
    merge_index_array_then_sort = DAG(
        {
            "files_hash_dict": Task(numpygraph.load.edge_count_sum_chunk, "$context"),
            "files_freq_dict": Task(numpygraph.load.edge_count_sum_freq, "$context"),
            "edges_count_sum": Task(numpygraph.load.summing, "$files_hash_dict", "$files_freq_dict"),
            "mergeindex": Task(numpygraph.load.MergeIndex_gen, "$context", "$edges_count_sum"),
            "merge_idx_to_gen": Task(numpygraph.load.merge_idx_to_gen, "$mergeindex", "$files_hash_dict"),
            "merge_freq_idx_to_gen": Task(numpygraph.load.merge_freq_idx_to_gen, "$mergeindex", "$files_freq_dict"),
            "merge_idx_to": MultiProcessTask(processpool, MergeIndex.merge_idx_to_wrapper, "$merge_idx_to_gen"),
            "merge_freq_idx_to": MultiProcessTask(processpool, MergeIndex.merge_freq_idx_to_wrapper,
                                                  "$merge_freq_idx_to_gen"),
            "dump": Task(MergeIndex.freq_idx_pointer_dump, "$mergeindex", "$context"),
            "Ending": EndTask(end, "$merge_idx_to", "$merge_freq_idx_to", "$dump")
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        merge_index_array_then_sort.execute(pool).get()

    # hid idx merge
    hid_idx_merge = DAG(
        {
            "gen": Task(numpygraph.load.hid_idx_dict_gen, "$graph"),
            "hid_idx_merge_run": MultiProcessTask(processpool, numpygraph.load.hid_idx_dict, "$gen"),
            "Ending": EndTask(end, "$hid_idx_merge_run")
        }
    )(graph=context.graph)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        hid_idx_merge.execute(pool).get()


if __name__ == "__main__":
    # import tinydag
    # tinydag.tinydag.check([1,2])
    # tinydag.tinydag.check_custom_class()

    dataset_path, graph_path, node_type_cnt, node_cnt, edge_cnt = "_dataset_test_directory", "_graph", 5, 1000, 10000


    def mock():
        # 生成测试样本
        from numpygraph.datasets.make_neo4j_csv import graph_generator
        os.system(f"mkdir -p {dataset_path}")
        graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
        os.system(f"head -2 {dataset_path}/*.csv")
        os.system(f"wc -l {dataset_path}/*.csv")


    def clean():
        # Clean up
        os.system(f"rm -r {dataset_path}")
        os.system(f"rm -r {graph_path}")
        os.system(f"find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete")


    # clean()
    # mock()
    stime = time.time()
    dag_load(dataset_path, graph_path)
    etime = time.time()
    print(f"Time usage:", etime - stime)
    # clean()
