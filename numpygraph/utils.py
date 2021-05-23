import sys
import uuid
from dask.distributed import Client


def globalize(func):
    def result(*args, **kwargs):
        return func(*args, **kwargs)

    result.__name__ = result.__qualname__ = uuid.uuid4().hex
    setattr(sys.modules[result.__module__], result.__name__, result)
    return result


def connect_client():
    # return Client("tcp://192.168.7.51:8786")
    return Client("tcp://192.168.6.171:8786")


def init_workder(client):
    # client = connect_client()
    from dask.distributed import PipInstall

    # plugin = PipInstall(
    #     packages=["/data/codes/numpygraph/dist/numpygraph-0.0.1.3.zip"]
    # )
    # client.register_worker_plugin(plugin)
    client.restart()
    client.upload_file("dist/numpygraph-0.0.1.3-py3.7.egg")
    # client.upload_file("dist/numpygraph-0.0.1.3.zip")


# class Timer:
#     def __enter__(self):
#         self.begin = now()

#     def __exit__(self, type, value, traceback):
#         print(format_delta(self.begin, now()))
