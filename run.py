import argparse
import os
import sys
from pathlib import Path

import formatted
import exploitation


def retrive(args: argparse.Namespace):
    raise NotImplementedError("For a landing zone implemeentation refer to delivery 1.")


def commit(args: argparse.Namespace):
    if args.reset:
        if len(args.pipe) == 0:
            print("Are you sure you want to reset all pipelines in formatted? (y/n)")
            if input().lower()[:1] != "y":
                print("Canceled")
                return
        formatted.reset(*args.pipe)
    formatted.commit(*args.pipe)


def extract(args: argparse.Namespace):
    if args.reset:
        if len(args.pipe) == 0:
            print("Are you sure you want to reset all pipelines in exploitation? (y/n)")
            if input().lower()[:1] != "y":
                print("Canceled")
                return
        exploitation.reset(*args.pipe)
    exploitation.extract(*args.pipe)


def main(input_: list[str]):
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="cmd")
    retrive_cmd = subparsers.add_parser("retrive")
    retrive_cmd.set_defaults(func=retrive)
    commit_cmd = subparsers.add_parser("commit")
    commit_cmd.add_argument(
        "--pipe",
        type=lambda x: x.split(","),
        metavar="PIPE1,...",
        default=[],
        help="Run commit only on the specified comma separated pipes",
    )
    commit_cmd.add_argument(
        "--reset",
        action="store_true",
        help="Reset the files from the selected pipelines",
    )
    commit_cmd.set_defaults(func=commit)
    extract_cmd = subparsers.add_parser("extract")
    extract_cmd.add_argument(
        "--pipe",
        type=lambda x: x.split(","),
        metavar="PIPE1,...",
        default=[],
        help="Run extract only on the specified comma separated pipes",
    )
    extract_cmd.add_argument(
        "--reset",
        action="store_true",
        help="Reset the files from the selected pipelines",
    )
    extract_cmd.set_defaults(func=extract)

    args = parser.parse_args(input_)
    if args.cmd is None:
        parser.print_help()
    else:
        os.chdir(Path(__file__).absolute().parent)
        args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
