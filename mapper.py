#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import sjoin
from pandas import concat, DataFrame
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
        self.relation = config.get_relation()

        self.info_logger = logger
        self.result_logger = ResultLogger('ResultLogger', source_sparql.get_query_hash(), target_sparql.get_query_hash())

    def map(self, to_file=True):
        self.info_logger.logger.log(INFO, "Mapping started...")
        start = time.time()

        if self.relation != 'distance' and self.relation != 'hausdorff_distance':
            results = sjoin(self.source, self.target, how='inner', op=self.relation)

            if self.relation == 'contains':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfContains')
            elif self.relation == 'covered_by':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#')
            elif self.relation == 'covers':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#ehCovers')
            elif self.relation == 'crosses':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfCrosses')
            elif self.relation == 'disjoint':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfDisjoint')
            elif self.relation == 'intersects':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfIntersects')
            elif self.relation == 'overlaps':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfOverlaps')
            elif self.relation == 'touches':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfTouches')
            elif self.relation == 'within':
                results.insert(1, 'relation', 'http://www.opengis.net/ont/geosparql#sfWithin')
        else:
            results = DataFrame(columns=[self.config.get_var_uri('source'), self.config.get_var_uri('target'), 'distance'])

            for index, row in self.source.iterrows():
                row_results = DataFrame()
                source_shape = row[self.config.get_var_shape('source')]

                if self.relation == 'distance':
                    self.target['distance'] = self.target.apply(lambda item: source_shape.distance(item[self.config.get_var_shape('target')]), axis=1)
                elif self.relation == 'hausdorff_distance':
                    self.target['distance'] = self.target.apply(lambda item: source_shape.hausdorff_distance(
                        item[self.config.get_var_shape('target')]), axis=1)

                current_results = self.target[self.target['distance'] < self.config.get_threshold()]
                row_results[self.config.get_var_uri('target')] = current_results[self.config.get_var_uri('target')]
                row_results['distance'] = current_results['distance']
                row_results[self.config.get_var_uri('source')] = row[self.config.get_var_uri('source')]

                if len(row_results) > 0:
                    results = concat([results, row_results], sort=False)

            results.insert(1, 'relation', 'http://jena.apache.org/spatial#nearby')

        formatted_results = self.convert(results)
        end = time.time()

        self.info_logger.logger.log(INFO, "Mapping took: {}s".format(round(end - start, 4)))
        self.info_logger.logger.log(INFO, "{} mappings found".format(len(results)))

        if to_file:
            self.result_logger.logger.info(formatted_results)

        return formatted_results

    def convert(self, results):
        formatted_results = None
        output_format = self.config.get_output_format()

        if output_format == 'turtle':
            results.insert(2, 'end', '.')
            results[self.config.get_var_uri('source')] = '<' + results[self.config.get_var_uri('source')].astype(str) + '>'
            results[self.config.get_relation()] = '<' + results[self.config.get_relation()].astype(str) + '>'
            results[self.config.get_var_uri('target')] = '<' + results[self.config.get_var_uri('target')].astype(str) + '>'
            formatted_results = results.to_csv(columns=[self.config.get_var_uri('source'), 'relation', self.config.get_var_uri(
                'target'), 'end'], header=False, index=False, index_label=False, sep=' ')
        elif output_format == 'json':
            formatted_results = results.to_json(na='drop')

        else:
            results.rename(columns={self.config.get_var_uri('source'): 'source_uri', self.config.get_var_uri('target'): 'target_uri'}, inplace=True)

            if 'distance' in results:
                formatted_results = results.to_csv(columns=['source_uri', 'relation', 'target_uri',
                                                            'distance'], header=True, index=False, index_label=False)
            else:
                formatted_results = results.to_csv(columns=['source_uri', 'relation', 'target_uri'], header=True, index=False, index_label=False)

        return formatted_results
