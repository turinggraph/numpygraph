"""Lite version of scipy.linalg.
Notes
-----
Reference: 
https://neo4j.com/docs/operations-manual/current/tutorial/neo4j-admin-import/
https://neo4j.com/docs/cypher-manual/current/syntax/values/
The property types:
Number, an abstract type, which has the subtypes Integer and Float
String
Boolean
The spatial type Point
Temporal types: Date, Time, LocalTime, DateTime, LocalDateTime and Duration
"""
import sys
import random
import pandas as pd
import numpy as np
import string
import time
from multiprocessing import Pool, cpu_count


base_string = string.digits + string.ascii_lowercase + string.ascii_uppercase


def random_string(_len):
    return "".join(random.sample(base_string, _len))


def generate_nodes_inner(node_cnt, node_type, graph_path):
    nodes = [
        (
            random_string(8)
            + node_type,  # TODO: mock type embedding has any magic usage?
            int(time.time()),
            random.uniform(0, 100),
            bool(random.getrandbits(1)),
            random_string(random.randint(3, 5)),
        )
        for _ in range(node_cnt)
    ]

    pd.DataFrame(nodes).to_csv(
        f"{graph_path}/node_{node_type}.csv",
        index=False,
        header=[
            f"{node_type}({node_type})",
            "attr1:int",
            "attr2:float",
            "attr3:bool",
            "attr4:str",
        ],
        mode="w+",
    )
    return [n[0] for n in nodes]


def generate_edge_inner(
    _id,
    source_nodes,
    target_nodes,
    node_cnt,
    edge_cnt,
    source_type,
    target_type,
    graph_path,
):

    source_nodes_len = len(source_nodes)
    target_nodes_len = len(target_nodes)
    pd.DataFrame(
        [
            (
                source_nodes[np.random.randint(0, source_nodes_len)],
                target_nodes[np.random.randint(0, target_nodes_len)],
                int(time.time()) % 10000,
                random.uniform(0, 100),
                bool(random.getrandbits(1)),
                random_string(random.randint(3, 5)),
            )
            for _ in range(edge_cnt)
        ]
    ).to_csv(
        f"{graph_path}/relation_{source_type}_{target_type}.csv",
        index=False,
        header=[
            f"{source_type}({source_type})",
            f"{target_type}({target_type})",
            "attr1:int",
            "attr2:float",
            "attr3:bool",
            "attr4:str",
        ],
        mode="w+",
    )


def graph_generator(graph_path, node_type_cnt, node_cnt, edge_cnt):
    node_types = [
        random_string(np.random.randint(3, 5)) for _ in range(node_type_cnt)
    ]
    pool = Pool(cpu_count())
    nodes = pool.starmap(
        generate_nodes_inner,
        [(node_cnt, node_types[i], graph_path) for i in range(node_type_cnt)],
    )

    pool.starmap(
        generate_edge_inner,
        sum(
            [
                [
                    (
                        f"{type_2}_{type_1}",
                        nodes[type_2],
                        nodes[type_1],
                        node_cnt,
                        edge_cnt,
                        node_types[type_2],
                        node_types[type_1],
                        graph_path,
                    )
                    for type_1 in range(type_2)
                ]
                for type_2 in range(node_type_cnt)
            ],
            [],
        ),
    )

    pool.close()


if __name__ == "__main__":
    dataset_path, node_type_cnt, node_cnt, edge_cnt = (
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
    )
    graph_generator(dataset_path, node_type_cnt, node_cnt, edge_cnt)
