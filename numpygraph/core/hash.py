# HASH_SHORT = 64
NODE_TYPE_MASK = 2 ** 60 - 1


def chash(t, s):
    """
    The hashing function of numpygraph.

    The least significant 60 bits are t | hash(s), while the other more significant bits are t

    :type t: any
    :param t: node type, usually
    :type s: any
    :param s: node value, usually

    :return: hash value
    """
    return t | (hash(s) & NODE_TYPE_MASK)
