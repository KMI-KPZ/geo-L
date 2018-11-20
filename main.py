#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from json import JSONDecodeError, loads
from os import makedirs
from os.path import exists, isdir
from sys import path
from urllib.error import HTTPError

from cache import Cache
from config import Config, ConfigNotValidError, load_config
from logger import InfoLogger
from mapper import Mapper
from sparql import SPARQL

path.append("${HOME}/.local/lib/python3.7/site-packages/")


def get_arguments():
    parser = ArgumentParser(description="Python LIMES")
    parser.add_argument("-c", "--config", type=str, dest="config_file", help="Path to a config file", required=True)
    parser.add_argument("-v", "--version", action="version", version="0.0.1", help="Show program version and exit")
    arguments = parser.parse_args()
    return arguments.config_file


def create_dirs():
    if not exists('cache') or not isdir('cache'):
        makedirs('cache')

    if not exists('logs') or not isdir('logs'):
        makedirs('logs')

    if not exists('output') or not isdir('output'):
        makedirs('output')


def run(config_string, to_file=True):
    create_dirs()
    results = None

    try:
        config_json = loads(config_string)
        config = Config(config_json)

        source_sparql = SPARQL(config, 'source')
        target_sparql = SPARQL(config, 'target')

        info_logger = InfoLogger('InfoLogger', '{}_{}'.format(source_sparql.get_query_hash(), target_sparql.get_query_hash()))

        source_cache = Cache(info_logger,  config, source_sparql, 'source')
        source = source_cache.create_cache()

        target_cache = Cache(info_logger, config, target_sparql, 'target')
        target = target_cache.create_cache()

        mapper = Mapper(info_logger, config, source_sparql, target_sparql, source, target)
        results = mapper.map(to_file)
    except ConfigNotValidError as e:
        results = "Config not valid"
        print(e)
    except HTTPError as e:
        print(e)
    except JSONDecodeError as e:
        print(e)

    return results


def main():
    try:
        connfig_file_path = get_arguments()
        config = load_config(connfig_file_path)
        run(config)
    except FileNotFoundError as e:
        print(e)


if __name__ == "__main__":
    main()
