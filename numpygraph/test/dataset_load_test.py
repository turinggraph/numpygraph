import random
import glob
import os
from numpygraph.load import load
from numpygraph.read import Read
from numpygraph.datasets.make_neo4j_csv import graph_generator
from numpygraph.context import Context
import cProfile

dataset_path, graph_path, node_type_cnt, node_cnt, edge_cnt = (
    "_dataset_test_directory",
    "graph",
    5,
    10000,
    10000000,
)


def mock():
    # 生成测试样本
    os.system(f"mkdir -p {dataset_path}")
    graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
    os.system(f"head -2 {dataset_path}/*.csv")
    os.system(f"wc -l {dataset_path}/*.csv")


def dump():
    # LOAD: dataset -> graph
    load(dataset_path, graph_path)


def sample():
    MAX_LEN = 20
    print(Context.node_attr_name, Context.node_attr_type)
    print(Context.NODE_TYPE)
    # READ: graph
    read = Read(dataset_path, graph_path)
    # Sample node, id
    sample_file = random.sample(glob.glob(f"{dataset_path}/relation_*.csv"), 1)[
        0
    ]
    sample_node_type = os.path.basename(sample_file).split("_")[1]
    random_line = random.sample(open(sample_file).readlines(), 1)[0].strip("\n")
    print("Edge:", random_line)
    sample_node_value = random_line.split(",")[0]
    sample_node_id = read.fetch_node_id(sample_node_type, sample_node_value)
    another_node_type = (
        os.path.basename(sample_file).split("_")[2].split(".")[0]
    )
    another_node_value = random_line.split(",")[1]
    another_node_id = read.fetch_node_id(another_node_type, another_node_value)
    print(
        "Sample Node Type:",
        sample_node_type,
        "\nSample Node value:",
        sample_node_value,
    )
    print("Sample Node ID:", sample_node_id)
    print(
        "Another Node Type:",
        another_node_type,
        "\nAnother Node value:",
        another_node_value,
    )
    print("Another Node ID:", another_node_id)
    print("Node Attr:", read.fetch_node_attr(sample_node_id))
    neighbor_nodes = read.fetch_node_neibor_nodes(sample_node_id)
    print("Neighbor nodes:", neighbor_nodes[:MAX_LEN])
    print("Neighbor nodes' attrs:")
    for neighbor_node in neighbor_nodes[:MAX_LEN]:
        print(read.fetch_node_attr(neighbor_node))
    print("Edge Attr:", read.fetch_edge_attr(sample_node_id, another_node_id))
    edges_sample = read.fetch_node_neibor_edges(sample_node_id)
    print("Neighbor Edges:", len(edges_sample), edges_sample[:MAX_LEN])
    print(
        "Randomly selected edges around a certain node:",
        read.random_sample_nodes(sample_node_id, MAX_LEN),
    )
    print(
        "Sample nodes with degree:",
        read.sample_node_with_degree(sample_node_id),
    )


def clean(paths=[]):
    # Clean up
    for p in paths:
        os.system(f"rm -r {p}")
    # os.system(f"rm -r {graph_path}")
    os.system(
        f"find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete"
    )


def test_pipeline(debug=False):
    if not debug:
        clean([dataset_path, graph_path])
        mock()
        dump()
        sample()
        clean()
    else:
        clean([graph_path])
        mock()
        dump()
        sample()


if __name__ == "__main__":
    cProfile.run("test_pipeline(True)", filename="result.out")
    # test_pipeline()