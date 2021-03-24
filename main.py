from npgraph.load import load
from npgraph.read import Read


def main():
    argv = [
        "/Users/hanry/Documents/npgraph/data/graphset_lite_attr.csv",
        "/Users/hanry/Documents/npgraph/data/graph",
        3
    ]

    load(argv[0], argv[1])

    Read.init(argv[0], argv[1])

    print(Read.node_info("e548369d-a0f9-4ed4-8be3-c13963b57fa7", "event_id", -1))


if __name__ == "__main__":
    main()
