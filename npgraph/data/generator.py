import random
import pandas as pd
import numpy as np
import string
import time

base_string = string.digits + string.ascii_lowercase + string.ascii_uppercase


def random_string(len):
    return "".join(np.random.choice(list(base_string), len))


def graph_generator(graph_path, node_type_cnt, node_cnt, edge_cnt):
    node_types = [random_string(np.random.randint(5, 10)) for _ in range(node_type_cnt)]
    nodes = [[random_string(13) + node_types[i] for _ in range(node_cnt)]
             for i in range(node_type_cnt)]
    edges = [
        [
            [
                (nodes[type_1][np.random.randint(0, node_cnt)], nodes[type_2][np.random.randint(0, node_cnt)],
                 int(time.time()),
                 random.uniform(0, 100),
                 bool(random.getrandbits(1)),
                 random_string(random.randint(5, 10))
                 )
                for _ in range(edge_cnt)
            ]
            for type_1 in range(type_2)
        ]
        for type_2 in range(node_type_cnt)

    ]

    for i in range(node_type_cnt):
        pd.Series(nodes[i]).to_csv(f"{graph_path}/node_{node_types[i]}.csv", index=False,
                                   header=[f"{node_types[i]}"], mode="w+")
    for j in range(i):
        pd.DataFrame(edges[i][j]).to_csv(f"{graph_path}/relation_{node_types[i]}_{node_types[j]}.csv", index=False,
                                         header=[f"{node_types[i]}", f"{node_types[j]}", "ts", "FLOAT", "BOOL",
                                                 "STRING"], mode="w+")


if __name__ == "__main__":
    graph_generator("info", 5, 10, 20)
