# HASH_SHORT = 64
# NODE_TYPE_MASK = 2 ** 60 - 1
NODE_TYPE_MASK = 2 ** 59 - 1


def chash(t, s):
    # the least significant 60 bits are t | hash(s), while the other more significant bits are t
    return t | (hash(s) & NODE_TYPE_MASK)
