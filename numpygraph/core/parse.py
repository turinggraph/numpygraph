import numpy as np


class Parse:
    data_type = {
        "int": np.int,
        "int64": np.int64,
        "int32": np.int32,
        "float": np.float,
        "bool": np.bool,
        "boolean": np.bool,
        "str": np.str,
        "string": np.str,
    }

    type_data = {v:k for k,v in data_type.items()}
    @staticmethod
    def str2bool(str_value):
        return str_value.lower() in ("yes", "true", "t", "1")

    @staticmethod
    def get_type(type_as_string):
        return Parse.data_type[type_as_string]

    @staticmethod
    def get_type_reverse(data_type):
        return Parse.type_data[data_type]

    @staticmethod
    def get_value(value_type, value_as_string):
        if value_type is np.bool:
            return Parse.str2bool(value_as_string)
        if value_type is np.str:
            return None
        return value_type(value_as_string)
