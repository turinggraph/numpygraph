import random
import glob
import os
from numpygraph.dag_load import dag_load
from numpygraph.read import Read
from numpygraph.datasets.make_neo4j_csv import graph_generator

dataset_path, graph_path, node_type_cnt, node_cnt, edge_cnt = "_dataset_test_directory", "_graph", 5, 1000, 10000


def mock():
    """
    Generate node files and relationship files.
    """
    # 生成测试样本
    os.system(f"mkdir -p {dataset_path}")
    graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
    os.system(f"head -2 {dataset_path}/*.csv")
    os.system(f"wc -l {dataset_path}/*.csv")


def dump():
    """
    Step that load all nodes and relations.

    :return: context after all loading process
    """
    # LOAD: dataset -> graph
    return dag_load(dataset_path, graph_path)


def sample(context):
    """
    Step that attempts to read from numpygraph database.

    If everything is working, this function should print:
        * "Edge:" a random relation
        * "Sample Node Type:" a first node from the relation
        * "Sample Node ID:"  node id (node hash) of this node
        * "Another Node Type:" the other node from the relation (the second node)
        * "Another Node ID:" node id of the other node
        * "Node Attr:" attributes of the first node (excluding strs)
        * "Neighbor nodes:" neighbor nodes of the first node
        * "Neighbor nodes' attrs:" attribute of the neighbor nodes
        * "Edge Attr:" find the attribute of the edge between the two previous nodes
        * "Neighbor Edges:" edges incident to the first node
        * "Randomly selected edges around a certain node:" ... around the first node
        * "Sample nodes with degree:": starting from the first node; do a depth search of depth 3, limiting the number of nodes in each level to be 4; print all nodes traveled in the depth tree

    :type context: :class:`numpygraph.context.Context`
    :param context: context returned from dump() function

    :return: no return value
    """
    # READ: graph
    read = Read(dataset_path, graph_path, context)
    # Sample node, id
    sample_file = random.sample(glob.glob(f"{dataset_path}/relation_*.csv"), 1)[0]
    sample_node_type = os.path.basename(sample_file).split("_")[1]
    random_line = random.sample(open(sample_file).readlines(), 1)[0].strip('\n')
    print("Edge:", random_line)
    sample_node_value = random_line.split(",")[0]
    sample_node_id = read.fetch_node_id(sample_node_type, sample_node_value)
    another_node_type = os.path.basename(sample_file).split("_")[2].split('.')[0]
    another_node_value = random_line.split(",")[1]
    another_node_id = read.fetch_node_id(another_node_type, another_node_value)
    print("Sample Node Type:", sample_node_type, "\nSample Node value:", sample_node_value)
    print("Sample Node ID:", sample_node_id)
    print("Another Node Type:", another_node_type, "\nAnother Node value:", another_node_value)
    print("Another Node ID:", another_node_id)
    print("Node Attr:", read.fetch_node_attr(sample_node_id))
    neighbor_nodes = read.fetch_node_neibor_nodes(sample_node_id)
    print("Neighbor nodes:", neighbor_nodes)
    print("Neighbor nodes' attrs:")
    for neighbor_node in neighbor_nodes:
        print(read.fetch_node_attr(neighbor_node))
    print("Edge Attr:", read.fetch_edge_attr(sample_node_id, another_node_id))
    print("Neighbor Edges:", read.fetch_node_neibor_edges(sample_node_id))
    print("Randomly selected edges around a certain node:", read.random_sample_nodes(sample_node_id, 10))
    print("Sample nodes with degree:", read.sample_node_with_degree(sample_node_id))


def clean():
    """
    Clean up all files generated (include relation files, node files, and log files (from tinydag))
    """
    # Clean up
    os.system(f"rm -r {dataset_path}")
    os.system(f"rm -r {graph_path}")
    os.system(f"find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete")
    os.system("rm log/dag_test.log")
    os.system("rm -d log")


def test_pipeline():
    """
    Test pipeline for numpygraph
    """
    clean()
    mock()
    context = dump()
    sample(context)
    clean()
