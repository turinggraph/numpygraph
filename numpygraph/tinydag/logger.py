from flask import Flask
from flask_cors import CORS
app = Flask(__name__)


CORS(app)

LOG_DIRECTORY = "log"
import json
import glob
import os
import re
from flask import Response
from collections import defaultdict


@app.route("/")
def index():
    """
    取全部历史的dag id
    """
    fmt = re.compile("dag_(.*).log")
    r = json.dumps(
        [
            fmt.findall(os.path.basename(f))[0]
            for f in glob.glob(f"{LOG_DIRECTORY}/dag_*.log")
        ]
    )
    return Response(r, content_type="application/json; charset=utf-8")


@app.route("/log/<id>")
def log(id):
    """
    取对应id的日志
    """
    r = str(open(f"{LOG_DIRECTORY}/dag_{id}.log").read())
    return Response(r, content_type="text/plain; charset=utf-8")


def get_records(r):
    records = []
    for line in r.split("\n"):
        try:
            obj = line.split("- INFO - ")[1]
            r = json.loads(obj)
            r['datetime'] = line.split(" - ")[0] 
            records.append(r)
        except IndexError:
            pass
    return records


def filter_records(records, rules):
    return filter(
        lambda r: all(r.get(rk, "") == rv for rk, rv in rules.items()), records
    )


def get_schema(r):
    return list(filter_records(get_records(r), {"type": "InitSchema"}))


def get_state(r):
    variable_state_records = list(
        filter_records(get_records(r), {"type": "VariableStateUpdate"})
    )
    variable_state = defaultdict(list)
    for state in variable_state_records:
        _id = state["context"] + state["name"]
        variable_state[_id].append(state)
    task_state_records = list(
        filter_records(get_records(r), {"type": "UpdateTaskState"})
    )
    task_state = defaultdict(list)
    for state in task_state_records:
        task_state[state["id"]].append(state)
    return {"variable": variable_state, "task": task_state}
    pass


@app.route("/schema/<id>")
def schema(id):
    """
    取对应id的日志, 依据时序排序取最新的schema
    """
    r = str(open(f"{LOG_DIRECTORY}/dag_{id}.log").read())
    r = json.dumps(get_schema(r))
    return Response(r, content_type="application/json; charset=utf-8")


@app.route("/state/<id>")
def state(id):
    """
    取对应id的日志, 依据时序排序取最新的node状态
    """
    r = str(open(f"{LOG_DIRECTORY}/dag_{id}.log").read())
    state = json.dumps(get_state(r))
    return Response(state, content_type="application/json; charset=utf-8")


@app.route("/user/<username>")
def profile(username):
    return f"{username}'s profile"


# with app.test_request_context():
#     print(url_for("index"))
#     print(url_for("login"))
#     print(url_for("login", next="/"))
#     print(url_for("profile", username="John Doe"))
if __name__ == "__main__":
    app.run(host="0.0.0.0")
