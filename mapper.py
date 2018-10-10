#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import sjoin
from logging import INFO

from logger import InfoLogger, ResultLogger

import time


class Mapper:
    def __init__(self, logger, config, source_sparql, target_sparql, source, target):
        self.config = config
        self.source_sparql = source_sparql
        self.target_sparql = target_sparql
        self.source = source
        self.target = target
        self.measures = config.get_measures()

        self.info_logger = logger
        self.result_logger = ResultLogger('ResultLogger', source_sparql.get_query_hash(), target_sparql.get_query_hash())

    def map(self, to_file=True):
        self.info_logger.logger.log(INFO, "Mapping started...")
        start = time.time()
        results = sjoin(self.source, self.target, how='inner', op='within')
        end = time.time()

        self.info_logger.logger.log(INFO, "Mapping took: {}s".format(round(end - start, 4)))
        self.info_logger.logger.log(INFO, "{} mappings found".format(len(results)))

        formatted_results = self.convert(results)

        if to_file:
            self.result_logger.logger.info(formatted_results)

        return formatted_results

    def convert(self, results):
        formatted_results = None
        result_format = self.config.get_result_format()
        results.insert(1, 'within', 'http://www.opengis.net/ont/geosparql#sfWithin')

        if result_format == 'turtle':
            results.insert(2, 'end', '.')
            results[self.config.get_var_uri('source')] = '<' + results[self.config.get_var_uri('source')].astype(str) + '>'
            results['within'] = '<' + results['within'].astype(str) + '>'
            results[self.config.get_var_uri('target')] = '<' + results[self.config.get_var_uri('target')].astype(str) + '>'
            formatted_results = results.to_csv(columns=[self.config.get_var_uri('source'), 'within', self.config.get_var_uri(
                'target'), 'end'], header=False, index=False, index_label=False, sep=' ')
        else:
            formatted_results = results.to_csv(columns=[self.config.get_var_uri('source'), 'within',
                                                        self.config.get_var_uri('target')], header=False, index=False, index_label=False)

        return formatted_results
