import numpy as np


class Parse:
    """
    Converts attributes dtypes in str format in csv files to dtypes. Converts attributes from inconsistent str form to types that :class:`numpygraph.core.arraylist.Arraylist` and :class:`numpygraph.core.arraydict.ArrayDict` can store (ignoreing str types).
    """
    __data_type = {
        'int': np.int,
        'int64': np.int64,
        'int32': np.int32,
        'float': np.float,
        'bool': np.bool,
        'str': np.str
    }

    @staticmethod
    def __str2bool(str_value):
        return str_value.lower() in ("yes", "true", "t", "1")

    @staticmethod
    def get_type(type_as_str):
        """
        Convert dtype in str form to dtype.

        :type type_as_str: str
        :param type_as_str: dtype in str format
        :return: dtype
        """
        return Parse.__data_type[type_as_str]

    @staticmethod
    def get_value(value_type, value_as_str):
        """
        Convert attributes values from various types to built-in forms, ignoring types that ArrayDict and ArrayList cannot store.

        :type value_type: dtype
        :param value_type: type we want to parse the value to
        :type value_as_str: str
        :param value_as_str: value in str format
        :return: If value_type is str, return value would be None since ArrayDict and ArrayList don't hold str values; otherwise return value is the parsed value_as_str into type value_type.
        """
        if value_type is np.bool:
            return Parse.__str2bool(value_as_str)
        if value_type is np.str:
            return None
        return value_type(value_as_str)
