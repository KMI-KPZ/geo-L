#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logger import InfoLogger, ResultLogger
from logging import INFO
from pandas import DataFrame

import csv
import psycopg2
import time


class Mapper:
    def __init__(self, logger, config, source_sparql, target_sparql):
        self.config = config
        self.source_sparql = source_sparql
        self.target_sparql = target_sparql
        self.relation = config.get_relation()

        self.info_logger = logger
        self.result_logger = ResultLogger('ResultLogger', source_sparql.get_query_hash(), target_sparql.get_query_hash())

    def map(self, to_file=True):
        self.info_logger.logger.log(INFO, "Mapping started...")
        start = time.time()
        relation_function = 'ST_WITHIN'  # Default
        source_query_hash = self.source_sparql.get_query_hash()
        target_query_hash = self.target_sparql.get_query_hash()
        source_offset = self.config.get_offset('source')
        target_offset = self.config.get_offset('target')

        if self.config.get_limit('source') > 0:
            source_max_offset = source_offset + self.config.get_limit('source') - 1
            source_query = 'SELECT * FROM {} WHERE server_offset BETWEEN {} AND {} AND geo IS NOT NULL'.format(
                'table_' + source_query_hash, source_offset, source_max_offset)
        else:
            source_query = 'SELECT * FROM {} WHERE geo IS NOT NULL OFFSET {}'.format('table_' + source_query_hash, source_offset)

        if self.config.get_limit('target') > 0:
            target_max_offset = target_offset + self.config.get_limit('target') - 1
            target_query = 'SELECT * FROM {} WHERE server_offset BETWEEN {} AND {} AND geo IS NOT NULL'.format(
                'table_' + target_query_hash, target_offset, target_max_offset)
        else:
            target_query = 'SELECT * FROM {} WHERE geo IS NOT NULL OFFSET {}'.format('table_' + target_query_hash, target_offset)

        if self.relation == 'contains':
            relation_function = 'ST_CONTAINS'
        elif self.relation == 'contains_properly':
            relation_function = 'ST_CONTAINSPROPERLY'
        elif self.relation == 'covers':
            relation_function = 'ST_COVERS'
        elif self.relation == 'covered_by':
            relation_function = 'ST_COVEREDBY'
        elif self.relation == 'covers':
            relation_function = 'ST_CROSSES'
        elif self.relation == 'disjoint':
            relation_function = 'ST_DISJOINT'
        elif self.relation == 'intersects':
            relation_function = 'ST_INTERSECTS'
        elif self.relation == 'overlaps':
            relation_function = 'ST_OVERLAPS'
        elif self.relation == 'touches':
            relation_function = 'ST_TOUCHES'
        elif self.relation == 'within':
            relation_function = 'ST_WITHIN'
        elif self.relation == 'distance_within':
            relation_function = 'ST_DWITHIN'
        elif self.relation == 'distance':
            relation_function = 'ST_DISTANCE'
        elif self.relation == 'hausdorff_distance':
            relation_function = 'ST_HAUSDORFFDISTANCE'

        if self.relation == 'distance' or self.relation == 'hausdorff_distance':
            query = """
            SELECT {}(source_data.geo, target_data.geo) AS distance, source_data.\"{}\" AS source_uri, target_data.\"{}\" AS target_uri
            FROM ({}) AS source_data, ({}) AS target_data
            """.format(relation_function, self.config.get_var_uri('source'), self.config.get_var_uri('target'), source_query, target_query)
        else:
            query = """
            SELECT source_data.\"{}\" AS source_uri, target_data.\"{}\" AS target_uri
            FROM ({}) AS source_data
            INNER JOIN
            ({}) AS target_data
            ON {}(source_data.geo, target_data.geo)
            """.format(self.config.get_var_uri('source'), self.config.get_var_uri('target'), source_query, target_query, relation_function)

        connection = psycopg2.connect(self.config.get_database_string())
        cursor = connection.cursor()
        cursor.execute(query)

        if self.relation == 'distance' or self.relation == 'hausdorff_distance':
            data_frame = DataFrame(cursor.fetchall(), columns=['distance', 'source_uri', 'target_uri'])
        else:
            data_frame = DataFrame(cursor.fetchall(), columns=['source_uri', 'target_uri'])

        cursor.close()
        connection.close()
        data_frame.insert(1, 'relation', self.relation)
        end = time.time()

        self.info_logger.logger.log(INFO, "Mapping took: {}s".format(round(end - start, 4)))
        self.info_logger.logger.log(INFO, "{} mappings found".format(len(data_frame)))

        formatted_results = self.convert(data_frame)

        if to_file:
            self.result_logger.logger.info(formatted_results)

        return formatted_results

    def convert(self, results):
        formatted_results = None
        output_format = self.config.get_output_format()

        # TODO: Turtle output for distance measures
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
            if 'distance' in results or 'hausdorff_distance' in results:
                formatted_results = results.to_csv(columns=['source_uri', 'relation', 'target_uri', 'distance'],
                                                   header=True, index=False, index_label=False)
            else:
                formatted_results = results.to_csv(columns=['source_uri', 'relation', 'target_uri'],
                                                   header=True, index=False, index_label=False)

        return formatted_results
