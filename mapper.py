#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import sjoin
from logging import ERROR, INFO
from shapely.wkt import loads

from logger import ErrorLogger, InfoLogger, ResultLogger

import fiona
import time


class Mapper:
    def __init__(self, logger, config, source_sparql, target_sparql, source, target):
        self.config = config
        self.source_sparql = source_sparql
        self.target_sparql = target_sparql
        self.source = source
        self.target = target
        self.measures = config.get_measures()

        # Shared data necessary for RAM usage
        global target_data
        target_data = self.target

        self.error_logger = ErrorLogger('ErrorLogger', 'errors', '{}_{}'.format(source_sparql.get_query_hash(), target_sparql.get_query_hash()))
        self.info_logger = logger
        self.result_logger = ResultLogger('ResultLogger', source_sparql.get_query_hash(), target_sparql.get_query_hash())

    def map(self):
        self.info_logger.logger.log(INFO, "Mapping started...")
        start = time.time()
        results = sjoin(self.source, target_data, how='inner', op='within')
        end = time.time()

        self.info_logger.logger.log(INFO, "Mapping took: {}s".format(round(end - start, 4)))
        self.info_logger.logger.log(INFO, "{} mappings found".format(len(results)))

        print(results.head())
