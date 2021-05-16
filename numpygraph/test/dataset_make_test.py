# import os
# from numpygraph.datasets.make_neo4j_csv import graph_generator
# def test_make_neo4j_csv():
#     dataset_path, node_type_cnt, node_cnt, edge_cnt = "_dataset_test_directory", 5, 1000, 10000
#     os.system(f"mkdir -p {dataset_path}")
#     graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
#     os.system(f"head -2 {dataset_path}/*.csv")
#     os.system(f"wc -l {dataset_path}/*.csv")
#     os.system(f"rm -r {dataset_path}")
