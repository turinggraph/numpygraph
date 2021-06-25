from numpygraph.tinydag.tinydag import Task, EndTask, DAG, Logger
from numpygraph.tinydag.tinydag import MultiProcessTask

import numpygraph
import numpygraph.load
from numpygraph.context import Context
from numpygraph.mergeindex import MergeIndex

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import os
import time

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
    print("Load relations")
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

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        relationship2indexarray.execute(pool).get()

    # merge index array then sort
    print("merge index array then sort")
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
            "write_mergeindex": Task(MergeIndex.relation_dumper, "$mergeindex", "$merge_idx_to", "$merge_freq_idx_to"),
            "dump": Task(MergeIndex.freq_idx_pointer_dump, "$mergeindex", "$context", "$write_mergeindex"),
            "Ending": EndTask(end, "$dump")
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        merge_index_array_then_sort.execute(pool).get()

    # hid idx merge
    print("hid idx merge")
    hid_idx_merge = DAG(
        {
            "gen": Task(numpygraph.load.hid_idx_dict_gen, "$graph"),
            "hid_idx_merge_run": MultiProcessTask(processpool, numpygraph.load.hid_idx_dict, "$gen"),
            "Ending": EndTask(end, "$hid_idx_merge_run")
        }
    )(graph=context.graph)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        hid_idx_merge.execute(pool).get()

    # node loading
    print("node loading")

    # node2indexarray
    print("node2indexarray")
    node2indexarray = DAG(
        {
            "gen": Task(numpygraph.load.node2idxarr_gen, "$context"),
            "node2idxarr": MultiProcessTask(processpool, numpygraph.load.node2idxarr, "$gen"),
            "write": EndTask(numpygraph.load.write_context, "$context", "$node2idxarr")
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        node2indexarray.execute(pool).get()

    print("merge_node_index")
    merge_node_index = DAG(
        {
            "merge_node_cursor_dict_gen": Task(numpygraph.load.merge_node_cursor_dict_gen, "$context"),
            "merge_node_cursor_dict": MultiProcessTask(processpool, numpygraph.load.merge_node_cursor_dict,
                                                       "$merge_node_cursor_dict_gen"),
            "Ending": EndTask(end, "$merge_node_cursor_dict"),
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        merge_node_index.execute(pool).get()

    print("Load finished")

    return context


if __name__ == "__main__":
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


    clean()
    mock()
    os.system(f"rm -r {graph_path}")
    stime = time.time()
    dag_load(dataset_path, graph_path)
    etime = time.time()
    print(f"Time usage:", etime - stime)
    clean()
