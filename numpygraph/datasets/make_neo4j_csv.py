"""

Lite version of scipy.linalg.

Notes
-----
Reference: 
https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/
https://neo4j.com/docs/cypher-manual/current/syntax/values/

The property types:
    * Number, an abstract type, which has the subtypes Integer and Float
    * String
    * Boolean
    * The spatial type Point
    * Temporal types: Date, Time, LocalTime, DateTime, LocalDateTime and Duration
"""
import sys
import random
import pandas as pd
import numpy as np
import string
import time

base_string = string.digits + string.ascii_lowercase + string.ascii_uppercase


def __random_string(len):
    """
    Generate random string of given length.
    """
    return "".join(np.random.choice(list(base_string), len))


def graph_generator(dataset_graph, node_type_cnt, node_cnt, edge_cnt):
    """
    Generates csv files of relations and nodes.

    :type dataset_graph: str
    :param dataset_graph: path to write files
    :type node_type_cnt: int
    :param node_type_cnt: number of node types
    :type node_cnt: int
    :param node_cnt: number of nodes of each type
    :type edge_cnt: int
    :param edge_cnt: number of edges between each pair of node types
    :return: No return value
    """
    node_types = [__random_string(np.random.randint(3, 5)) for _ in range(node_type_cnt)]
    nodes = [
        [
            (
                __random_string(8) + node_types[i],
                int(time.time()),
                random.uniform(0, 100),
                bool(random.getrandbits(1)),
                __random_string(random.randint(3, 5))
            )
            for _ in range(node_cnt)
        ]
        for i in range(node_type_cnt)
    ]
    edges = [
        [
            [
                (
                    nodes[type_2][np.random.randint(0, node_cnt)][0],
                    nodes[type_1][np.random.randint(0, node_cnt)][0],
                    int(time.time()) % 10000,
                    random.uniform(0, 100),
                    bool(random.getrandbits(1)),
                    __random_string(random.randint(3, 5))
                )
                for _ in range(edge_cnt)
            ]
            for type_1 in range(type_2)
        ]
        for type_2 in range(node_type_cnt)
    ]

    for i in range(node_type_cnt):
        pd.DataFrame(nodes[i]).to_csv(f"{dataset_graph}/node_{node_types[i]}.csv", index=False,
                                      header=[f"{node_types[i]}({node_types[i]})", "attr1:int", "attr2:float",
                                              "attr3:bool", "attr4:str"],
                                      mode="w+")
        for j in range(i):
            pd.DataFrame(edges[i][j]).to_csv(f"{dataset_graph}/relation_{node_types[i]}_{node_types[j]}.csv",
                                             index=False,
                                             header=[f"{node_types[i]}({node_types[i]})",
                                                     f"{node_types[j]}({node_types[j]})",
                                                     "attr1:int",
                                                     "attr2:float",
                                                     "attr3:bool",
                                                     "attr4:str"
                                                     ], mode="w+")


if __name__ == "__main__":
    dataset_path, node_type_cnt, node_cnt, edge_cnt = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
