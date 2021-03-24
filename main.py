from npgraph.load import load


def main():
    argv = [
        "/Users/hanry/Documents/npgraph/data/graphset_lite_attr.csv/",
        "/Users/hanry/Documents/npgraph/data/graph",
        3
    ]

    load(argv[0], argv[1])


if __name__ == "__main__":
    main()
