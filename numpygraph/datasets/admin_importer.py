import os
import sys
import glob

path = "/nvme500g/_dataset_test_directory_5G"
node_cmd, edge_cmd = "", ""
for n in map(lambda s: os.path.basename(s), glob.glob(f"{path}/node_*.csv")):
    _label = n.split("_")[1].split(".")[0]
    node_cmd += f" --nodes={_label}={n} "

for r in map(
    lambda s: os.path.basename(s), glob.glob(f"{path}/relation_*.csv")
):
    _type = "_".join(r.split(".")[0].split("_")[1:])
    edge_cmd += f" --relationships={_label}={r} "

cmd = f"""
neo4j-admin import  --database=neo4j --delimiter="," \
    --skip-bad-relationships=true  \
    --ignore-extra-columns\
    --skip-duplicate-nodes\
    {node_cmd}\
    {edge_cmd}\
"""
print(cmd)
