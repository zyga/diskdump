#!/usr/bin/env python3

from argparse import ArgumentParser


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "disk", metavar="DISK", help="Block device to operate on")
    parser.add_argument(
        "dump", metavar="DUMP", help="Dump of the block device to operate on")
    group = parser.add_argument_group(title="action to perform")
    group = group.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c", "--create", help="Copy data from DISK to DUMP",
        dest="action", action="store_const", const="create")
    group.add_argument(
        "-t", "--test", help="Compare data from DISK and DUMP",
        dest="action", action="store_const", const="create")
    ns = parser.parse_args()
    print(ns)



if __name__ == "__main__":
    main()
