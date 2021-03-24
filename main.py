import glob
import time

from npgraph.context import Context
from npgraph.load import relationship2indexarray
from npgraph.load import merge_index_array_then_sort
from npgraph.load import hid_idx_merge
from npgraph.load import node2indexarray
from npgraph.load import merge_node_index


def main():
    argv = [
        "/Users/hanry/Documents/npgraph/data/graphset_lite_attr.csv/",
        "/Users/hanry/Documents/npgraph/data/graph",
        3
    ]
    stime = time.time()
    print("T1:", stime)
    dataset, graph = argv[0], argv[1]

    # Prepare relations
    Context.load_node_type_hash(f"{graph}/node_type_id.json")
    Context.load_node_file_hash(f"{graph}/node_file_id.json")
    Context.prepare_relations(glob.glob(f"{dataset}/relation_*.csv"))
    Context.prepare_nodes(glob.glob(f"{dataset}/node_*.csv"))

    # Process relationships
    print(f"Dataset: {dataset}\nGraph: {graph}")
    if False or (argv[2] & 0x01):  # Dump edge test
        print("Relationship Index Transforming...")
        relationship2indexarray(dataset, graph)
        print(f"Index resorted to edge mergeing...")
        merge_index_array_then_sort(graph)
        print(f"Graph index merge by hashid...")
        hid_idx_merge(graph)
        print("Merge success")

    # Process nodes
    if False or (argv[2] & 0x02):  # Dump node test
        print(f"Node to hashid transforming...")
        node2indexarray(dataset, graph)
        print(f"Node index mapping...")
        merge_node_index(graph)

    etime = time.time()
    print("T2:", etime)
    print("DT:", etime - stime)


if __name__ == "__main__":
    main()
