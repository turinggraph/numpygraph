from numpygraph.tinydag.tinydag import Task, EndTask, DAG, Logger

import numpygraph
import numpygraph.load
from numpygraph.context import Context

from concurrent.futures import ThreadPoolExecutor

logger = Logger()


def dag_load(dataset_path, graph_path):
    thread_num = 32
    print(f"Dataset: {dataset_path}\nGraph: {graph_path}")
    # load context
    context = Context().load_context(dataset_path, graph_path)

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
            "lines2idxarr": EndTask(numpygraph.load.relationship2indexarray, "$context", "$node_hash_space_stat"),
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
            "hid_idx_merge": EndTask(numpygraph.load.hid_idx_merge, "$graph")
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
            "node2indexarray": EndTask(numpygraph.load.node2indexarray, context),
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        node2indexarray.execute(pool).get()

    print("merge_node_index")
    merge_node_index = DAG(
        {
            "merge_node_index": EndTask(numpygraph.load.merge_node_index, "$context"),
        }
    )(context=context)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        merge_node_index.execute(pool).get()

    print("Load finished")

    return context
