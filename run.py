import argparse
import os
import sys
from pathlib import Path

import formatted


def retrive(args: argparse.Namespace):
    raise NotImplementedError("For a landing zone implemeentation refer to delivery 1.")


def commit(args: argparse.Namespace):
    formatted.commit()


def main(input_: list[str]):
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="cmd")
    retrive_cmd = subparsers.add_parser("retrive")
    retrive_cmd.set_defaults(func=retrive)
    commit_cmd = subparsers.add_parser("commit")
    commit_cmd.set_defaults(func=commit)

    args = parser.parse_args(input_)
    if args.cmd is None:
        parser.print_help()
    else:
        os.chdir(Path(__file__).absolute().parent)
        args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
