#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from sys import path

from config import load_config
from geolimes import goeLIMES

path.append("${HOME}/.local/lib/python3.7/site-packages/")


def get_arguments():
    parser = ArgumentParser(description="Python LIMES")
    parser.add_argument("-c", "--config", type=str, dest="config_file", help="Path to a config file", required=True)
    parser.add_argument("-d", "--database", type=str, dest="database_config_file", help="Path to a database config file", required=True)
    parser.add_argument("-v", "--version", action="version", version="0.0.1", help="Show program version and exit")
    arguments = parser.parse_args()
    return arguments.config_file, arguments.database_config_file


def main():
    try:
        connfig_file_path, database_config_file_path = get_arguments()
        config = load_config(connfig_file_path)
        database_config = load_config(database_config_file_path)
        limes = goeLIMES(database_config)
        limes.run(config)
    except FileNotFoundError as e:
        print(e)


if __name__ == "__main__":
    main()
