from numpygraph.tinydag.tinydag import Task, EndTask, DAG, Logger
from numpygraph.tinydag.tinydag import MultiProcessTask

import numpygraph
import numpygraph.load
from numpygraph.context import Context

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

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
            "lines2idxarr_arg_gen": Task(numpygraph.load.linesidxarr_arg_gen, "$context", "$node_hash_space_stat"),
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
            "merge_freq_and_other_idx_to": EndTask(numpygraph.load.merge_freq_and_other_idx_to,
                                                   "$context", "$files_hash_dict", "$files_freq_dict", "$mergeindex"),
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
