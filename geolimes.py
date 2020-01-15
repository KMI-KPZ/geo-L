#!/usr/bin/env python
# -*- coding: utf-8 -*-

from json import JSONDecodeError, loads
from os import makedirs
from os.path import exists, isdir
from urllib.error import HTTPError

from cache import Cache
from config import Config, ConfigNotValidError, load_config
from logger import InfoLogger
from mapper import Mapper
from sparql import SPARQL


class goeLIMES:
    def __init__(self, database_config):
        self.database_config = database_config

    def run(self, config_json, to_file=True):
        self.create_dirs()
        results = None

        try:
            config = Config(config_json, self.database_config)

            source_sparql = SPARQL(config, 'source')
            target_sparql = SPARQL(config, 'target')

            info_logger = InfoLogger('InfoLogger', '{}_{}'.format(source_sparql.get_query_hash(), target_sparql.get_query_hash()))

            source_cache = Cache(info_logger,  config, source_sparql, 'source')
            if config.get_endpoint_type('source') == 'remote': #0:
                source_cache.create_cache()
            elif config.get_endpoint_type('source') == 'local': #1:
                source_cache.create_cache_file()

            target_cache = Cache(info_logger, config, target_sparql, 'target')
            if config.get_endpoint_type('target') == 'remote': #0:
                target_cache.create_cache()
            elif config.get_endpoint_type('target') == 'local': #1:
                target_cache.create_cache_file()

            mapper = Mapper(info_logger, config, source_sparql, target_sparql)
            results = mapper.map(to_file)
        except ConfigNotValidError as e:
            results = "Config not valid"
            print(e)
        except HTTPError as e:
            print(e)
        except JSONDecodeError as e:
            print(e)

        return results

    def create_dirs(self):
        if not exists('logs') or not isdir('logs'):
            makedirs('logs')

        if not exists('output') or not isdir('output'):
            makedirs('output')
