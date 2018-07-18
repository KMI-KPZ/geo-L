#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from json import JSONDecodeError
from os import makedirs
from os.path import exists, isdir

from cache import Cache
from config import Config, ConfigNotValidError
from mapper import Mapper
from sparql import SPARQL


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


def main():
    create_dirs()

    try:
        connfig_file_path = get_arguments()
        config = Config(connfig_file_path)

        source_sparql = SPARQL(config, 'source')
        target_sparql = SPARQL(config, 'target')

        source_cache = Cache(config, source_sparql, 'source')
        source = source_cache.create_cache_chunks(False)

        target_cache = Cache(config, target_sparql, 'target')
        target = target_cache.create_cache_chunks(True)

        mapper = Mapper(config, source_sparql, target_sparql, source, target)
        mapper.map()
    except FileNotFoundError as e:
        print(e)
    except JSONDecodeError as e:
        print(e)
    except ConfigNotValidError as e:
        print(e)
    # except Exception as e:
    #    print(e)


if __name__ == "__main__":
    main()
