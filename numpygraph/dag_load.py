from tinydag.src.tinydag import Task, EndTask, DAG, Logger

import numpygraph
import numpygraph.load
from numpygraph.context import Context

from concurrent.futures import ThreadPoolExecutor

logger = Logger()


def end(*args, **kwargs):
    """
    Workaround function to put into EndTask.

    This function can be put into EndTask, taking several return values of previous Tasks so that the enclosing DAG
    would wait for all Tasks to end.

    :type args: any
    :param args: any return value of previous Tasks

    :type args: any
    :param kwargs: any return value of previous Tasks

    :return: always 0
    """
    print("ENDING...")
    print("*args:", *args)
    print("**kwargs", **kwargs)
    return 0


def split_helper(single_tuple, part):
    """
    Workaround function to split return values of Task.

    This function can be put into Task, returning the "part"'s element of the single_tuple that might be a mixed large tuple of several return values.

    :type single_tuple: any subscriptable object
    :param single_tuple: the mixed tuple

    :param part: int
    :return: the serial number of the desired part of the tuple to return
    """
    return single_tuple[part]


def dag_load(dataset_path, graph_path):
    """
    Loads the whole graph.

    This function wraps all loading functions inside a DAG (please refer to tinydag) containing individual load functions as tasks. File dependency of the tasks are solved by passing file lists between tasks and each task only fetching files they intend to read from arguments passed in.

    Internal details:
    There are three DAGs for relation loading and one DAG for node loading:

    * Relation loading:
        * ``relationship2indexarray``
        * ``merge_index_array_then_sort``
        * ``relation_loading`` that nests the previous two
    * Node loading:
        * ``node_loading``

    With all dependencies expressed in arguments that each Task takes and return values of the Tasks, the DAGs can be safely put together into the last DAG ``loading``.

    :type dataset_path: str
    :param dataset_path: path to dataset of relations and nodes.

    :type graph_path: str
    :param graph_path: path to the target directory where all intermediate result and dump files to be stored.

    :return: object :class:`numpygraph.context.Context` after import, containing info about node type hash, dataset_path, graph_path, etc.
    """
    thread_num = 32
    print(f"Dataset: {dataset_path}\nGraph: {graph_path}")
    # load context
    context = Context().load_context(dataset_path, graph_path)

    # relationships loading

    relationship2indexarray = DAG(
        {
            "lines_sampler": Task(numpygraph.load.lines_sampler, "$FILES_relation_files"),
            "node_hash_space_stat": Task(numpygraph.load.node_hash_space_stat, "$context", "$lines_sampler"),
            "lines2idxarr": EndTask(numpygraph.load.relationship2indexarray, "$context", "$node_hash_space_stat",
                                    "$FILES_relation_files"),
        }
    )(context="$context", FILES_relation_files="$FILES_relation_files")

    merge_index_array_then_sort = DAG(
        {
            "files_hash_dict": Task(numpygraph.load.edge_count_sum_chunk, "$FILES_normal_files"),
            "files_freq_dict": Task(numpygraph.load.edge_count_sum_freq, "$FILES_freq_files"),
            "edges_count_sum": Task(numpygraph.load.summing, "$files_hash_dict", "$files_freq_dict"),
            "mergeindex": Task(numpygraph.load.MergeIndex_gen, "$context", "$edges_count_sum"),
            "merge_freq_and_other_idx_to": EndTask(numpygraph.load.merge_freq_and_other_idx_to,
                                                   "$context", "$files_hash_dict", "$files_freq_dict", "$mergeindex"),
        }
    )(context="$context", FILES_normal_files="$normal_files", FILES_freq_files="$freq_files")

    relation_loading = DAG(
        {
            "relationship2indexarray": relationship2indexarray,
            "normal_files": Task(split_helper, "$relationship2indexarray", 0),
            "freq_files": Task(split_helper, "$relationship2indexarray", 1),
            "merge_index_array_then_sort": merge_index_array_then_sort,
            "edge_mapper_files": Task(split_helper, "$merge_index_array_then_sort", 0),
            "freq_idx_pointer_dump_path": Task(split_helper, "$merge_index_array_then_sort", 1),
            "hid_idx_merge": EndTask(numpygraph.load.hid_idx_merge, "$context", "$edge_mapper_files",
                                     "$freq_idx_pointer_dump_path"),
        }
    )(context="$context", FILES_relation_files="$FILES_relation_files")

    # node loading

    node_loading = DAG(
        {
            "node2indexarray": Task(numpygraph.load.node2indexarray, "$context", "$FILES_node_files"),
            "merge_node_index": EndTask(numpygraph.load.merge_node_index, "$context", "$node2indexarray")
        }
    )(context="$context", FILES_node_files="$FILES_node_files")

    loading = DAG(
        {
            "load_relation": relation_loading,
            "load_nodes": node_loading,
            "end": EndTask(end, "$load_relation", "$load_nodes")
        }
    )(context=context, FILES_relation_files=context.relation_files, FILES_node_files=context.node_files)

    with ThreadPoolExecutor(max_workers=thread_num) as pool:
        loading.execute(pool).get()

    print("Load finished")

    return context
