# HASH_SHORT = 64
NODE_TYPE_MASK = 2 ** 60 - 1


def chash(t, s):
    return t | (hash(s) & NODE_TYPE_MASK)
