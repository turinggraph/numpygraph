import numpy as np


class Parse:
    data_type = {
        "int": np.int,
        "int64": np.int64,
        "int32": np.int32,
        "float": np.float,
        "bool": np.bool,
        "str": np.str,
    }

    @staticmethod
    def str2bool(str_value):
        return str_value.lower() in ("yes", "true", "t", "1")

    @staticmethod
    def get_type(type_as_string):
        return Parse.data_type[type_as_string]

    @staticmethod
    def get_value(value_type, value_as_string):
        if value_type is np.bool:
            return Parse.str2bool(value_as_string)
        if value_type is np.str:
            return None
        return value_type(value_as_string)
