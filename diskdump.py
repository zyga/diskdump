#!/usr/bin/env python3
"""
Disk backup and restore utility.
"""

import argparse
import gzip
import io
import os
import stat
import multiprocessing


def parse_args(args=None):
    parser = argparse.ArgumentParser(args)
    parser.add_argument(
        "disk", metavar="DISK", help="Block device to operate on")
    parser.add_argument(
        "dump", metavar="DUMP", help="Dump of the block device to operate on")
    group = parser.add_argument_group(title="compression level")
    group.add_argument(
        "-0", "--store", help="Use no compression (just store)",
        dest="compresslevel", action="store_const", const=0)
    group.add_argument(
        "-1", "--fast", help="Use fastest compression",
        dest="compresslevel", action="store_const", const=1)
    for i in range(2, 9):
        group.add_argument(
            "-{}".format(i), help=argparse.SUPPRESS,
            dest="compresslevel", action="store_const", const=i)
    group.add_argument(
        "-9", "--best", help="Use best compression (slowest, smallest)",
        dest="compresslevel", action="store_const", const=9)
    group.set_defaults(compresslevel=6)
    group = parser.add_argument_group(title="action to perform")
    group = group.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-i", "--info", help="Information about DISK or DUMP",
        dest="action", action="store_const", const=cmd_info)
    group.add_argument(
        "-b", "--backup", help="Copy data from DISK to DUMP",
        dest="action", action="store_const", const=cmd_backup)
    group.add_argument(
        "-r", "--restore", help="Copy data from DUMP to DISK",
        dest="action", action="store_const", const=cmd_restore)
    group.add_argument(
        "-c", "--check", help="Compare data from DISK and DUMP",
        dest="action", action="store_const", const=cmd_check)
    group = parser.add_argument_group("display options")
    group.add_argument(
        '--1000', help="Define one kilo as 1000 (typical for disk sizes)",
        dest='get_human_size', action='store_const', const=get_human_size_1000)
    group.add_argument(
        '--1024', help="Define one kilo as 1024",
        dest='get_human_size', action='store_const', const=get_human_size_1024)
    group.set_defaults(get_human_size=get_human_size_1024)
    group = parser.add_argument_group("disk IO options")
    group.add_argument(
        '-B', '--block-size', help='Size of disk IO buffer',
        default=4<<20, type=int)
    ns = parser.parse_args()
    if ns.block_size < 1:
        parser.error("--block-size must be greater than 0")
    return ns


def get_human_size_1000(size):
    suffix_list = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for index, suffix in enumerate(suffix_list):
        human_size = size / (1000 ** index)
        if human_size < 1000:
            break
    return "{:.1f}{}".format(human_size, suffix)


def get_human_size_1024(size):
    suffix_list = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    for index, suffix in enumerate(suffix_list):
        human_size = size >> (10 * index)
        if human_size < 1024:
            break
    return "{:.1f}{}".format(human_size, suffix)


class ansi:

    black = "\033[30m"
    red = "\033[31m"
    green = "\033[32m"
    yellow = "\033[33m"
    blue = "\033[34m"
    magenta = "\033[35m"
    cyan = "\033[36m"
    white = "\033[37m"

    normal = "\033[0m"
    bright = "\033[1m"
    faint = "\033[2m"

    reset = "\033[0m"

    clear_line = "\033[K"


def cmd_info(ns):
    disk_stream = open(ns.disk, "rb") 
    dump_stream = gzip.open(ns.dump, "rb")
    try:
        disk_stat_result = os.fstat(disk_stream.fileno())
        dump_stat_result = os.fstat(dump_stream.fileno())
        if not stat.S_ISBLK(disk_stat_result.st_mode):
            raise ValueError("(disk) {} is not a block device".format(ns.disk))
        if not stat.S_ISREG(dump_stat_result.st_mode):
            raise ValueError("(dump) {} is not a regular file".format(ns.dump))
        disk_stream.seek(0, io.SEEK_END)
        disk_size = disk_stream.tell()
        dump_size = dump_stat_result.st_size
        print("Disk size: {} ({})".format(disk_size, get_human_size_1000(disk_size)))
        print("Dump size: {} ({})".format(dump_size, get_human_size_1000(dump_size)))
    finally:
        disk_stream.close()
        dump_stream.close()


def cmd_backup(ns):
    block_size = ns.block_size
    print("Block size: {} ({})".format(block_size, get_human_size_1024(block_size)))
    disk_stream = open(ns.disk, "rb") 
    dump_stream = gzip.open(ns.dump, "wb", compresslevel=ns.compresslevel)
    try:
        disk_stat_result = os.fstat(disk_stream.fileno())
        if not stat.S_ISBLK(disk_stat_result.st_mode):
            raise ValueError("(disk) {} is not a block device".format(ns.disk))
        disk_stream.seek(0, io.SEEK_END)
        disk_size = disk_stream.tell()
        disk_stream.seek(0)
        print("Disk size: {} ({})".format(disk_size, get_human_size_1000(disk_size)))
        block_count = (disk_size + block_size - 1) // block_size
        for block_id in range(block_count):
            print("\r{ansi.bright}{ansi.green}Reading{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            block = disk_stream.read(block_size)
            if block == '':
                print("??? EOF")
                break
            print("\r{ansi.bright}{ansi.red}Writing{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            dump_stream.write(block)
        assert disk_stream.read(block_size) == b''
    finally:
        disk_stream.close()
        dump_stream.close()
        print("\rBackup complete!{ansi.clear_line}".format(ansi=ansi))


def cmd_check(ns):
    block_size = ns.block_size
    print("Block size: {}".format(get_human_size_1024(block_size)))
    disk_stream = open(ns.disk, "rb") 
    dump_stream = gzip.open(ns.dump, "rb")
    try:
        disk_stat_result = os.fstat(disk_stream.fileno())
        if not stat.S_ISBLK(disk_stat_result.st_mode):
            raise ValueError("(disk) {} is not a block device".format(ns.disk))
        disk_stream.seek(0, io.SEEK_END)
        disk_size = disk_stream.tell()
        disk_stream.seek(0)
        print("Disk size: {} ({})".format(disk_size, get_human_size_1000(disk_size)))
        dump_stat_result = os.fstat(dump_stream.fileno())
        if not stat.S_ISREG(dump_stat_result.st_mode):
            raise ValueError("(dump) {} is not a regular file".format(ns.dump))
        dump_size = dump_stat_result.st_size
        print("Dump size: {} ({})".format(dump_size, get_human_size_1000(dump_size)))
        block_count = (disk_size + block_size - 1) // block_size
        for block_id in range(block_count):
            print("\r{ansi.bright}{ansi.green}Reading{ansi.normal}"
                  " {ansi.blue}disk{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            disk_block = disk_stream.read(block_size)
            print("\r{ansi.bright}{ansi.green}Reading{ansi.normal}"
                  " {ansi.magenta}disk{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            dump_block = dump_stream.read(block_size)
            if dump_block != disk_block:
                good = False
                print("Disk and dump don't match, block {}/{}".format(
                    block_id, block_count))
                break
            else:
                good = True
    finally:
        disk_stream.close()
        dump_stream.close()
    if good:
        print("\rBackup verification complete,"
              " {ansi.green}all good{ansi.normal}!"
              "{ansi.clear_line}".format(ansi=ansi))
    else:
        print("\rBackup verification failed,"
              " {ansi.red}different data{ansi.normal}!"
              "{ansi.clear_line}".format(ansi=ansi))
    return good 


def cmd_restore(ns):
    block_size = ns.block_size
    print("Block size: {} ({})".format(block_size, get_human_size_1024(block_size)))
    disk_fd = os.open(ns.disk, os.O_WRONLY|os.O_SYNC)
    dump_stream = gzip.open(ns.dump, "rb")
    try:
        disk_stat_result = os.fstat(disk_fd)
        if not stat.S_ISBLK(disk_stat_result.st_mode):
            raise ValueError("(disk) {} is not a block device".format(ns.disk))
        disk_size = os.lseek(disk_fd, 0, os.SEEK_END)
        os.lseek(disk_fd, 0, os.SEEK_SET)
        print("Disk size: {} ({})".format(disk_size, get_human_size_1000(disk_size)))
        dump_stat_result = os.fstat(dump_stream.fileno())
        if not stat.S_ISREG(dump_stat_result.st_mode):
            raise ValueError("(dump) {} is not a regular file".format(ns.dump))
        dump_size = dump_stat_result.st_size
        print("Dump size: {} ({})".format(dump_size, get_human_size_1000(dump_size)))
        block_count = (disk_size + block_size - 1) // block_size
        for block_id in range(block_count):
            print("\r{ansi.bright}{ansi.green}Reading{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            block = dump_stream.read(block_size)
            if block == b'':
                print("??? EOF")
                break
            print("\r{ansi.bright}{ansi.red}Writing{ansi.normal}"
                  " block {block_id}/{block_count}"
                  " ({ansi.cyan}{progress:.1f}%{ansi.normal}){ansi.clear_line}".format(
                      block_id=block_id,
                      block_count=block_count,
                      progress=100 * block_id / block_count,
                      ansi=ansi),
                  end='')
            os.write(disk_fd, block)
        assert dump_stream.read(block_size) == b''
    finally:
        os.close(disk_fd)
        dump_stream.close()
    print("\rRestore complete!{ansi.clear_line}".format(ansi=ansi))


def main(ns=None):
    if ns is None:
        ns = parse_args()
    return ns.action(ns)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
    except (IOError, NotImplementedError, ValueError) as exc:
        raise SystemExit(exc)
