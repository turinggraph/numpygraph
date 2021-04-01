import random
import glob
import os
from numpygraph.load import load
from numpygraph.read import Read
from numpygraph.datasets.make_neo4j_csv import graph_generator
dataset_path, graph_path, node_type_cnt, node_cnt, edge_cnt = "_dataset_test_directory", "graph", 5, 1000, 10000


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
    # READ: graph
    Read.init(dataset_path, graph_path)
    # Sample node, id
    sample_file = random.sample(glob.glob(f"{dataset_path}/relation_*.csv"), 1)[0]
    sample_node_type = os.path.basename(sample_file).split("_")[1]
    sample_node_id = random.sample(open(sample_file).readlines(), 1)[0].split(",")[0]
    print("Sample Node Type:", sample_node_type, "\nSample Node ID:", sample_node_id)
    print("Relationships:", Read.find_relationships(sample_node_id, sample_node_type, -1))
    print("Nodes:", Read.node_info(sample_node_id, sample_node_type, -1))


def clean():
    # Clean up
    os.system(f"rm -r {dataset_path}")
    os.system(f"rm -r {graph_path}")


def test_pipeline():
    clean()
    mock()
    dump()
    sample()
    clean()
