#!/usr/bin/env python
# -*- coding: utf-8 -*-

from csv import field_size_limit
from geopandas import GeoDataFrame
from io import StringIO
from logging import INFO
from os.path import join, isfile
from pandas import concat, read_csv
from shapely.wkt import loads
from sys import maxsize

from logger import InfoLogger

import pickle
import time


class Cache:
    def __init__(self, logger, config, sparql, type):
        self.config = config
        self.sparql = sparql
        self.type = type

        self.info_logger = logger

        self.set_max_field_size_limit()  # Needed for very long fields

    def create_cache(self):
        if isfile(join('cache', '{}.csv'.format(self.sparql.query_hash))):
            self.info_logger.logger.log(INFO, "Cache file {}.csv for query already exists".format(self.sparql.query_hash))
            self.info_logger.logger.log(INFO, "Loading cache file...")

            items = read_csv('cache/{}.csv'.format(self.sparql.query_hash))
            geo_data_frame = GeoDataFrame(items)
            geo_data_frame[self.config.get_var_shape(self.type)] = GeoDataFrame(
                geo_data_frame[self.config.get_var_shape(self.type)].apply(lambda x: loads(x)))
            geo_data_frame = geo_data_frame.set_geometry(self.config.get_var_shape(self.type))
            return geo_data_frame

        offset = self.config.get_offset(self.type)
        limit = self.config.get_limit(self.type)
        chunksize = self.config.get_chunksize(self.type)
        results = None
        run = True

        start = time.time()

        while(run):
            if limit > 0 and offset + chunksize >= limit:
                chunksize = limit - offset
                run = False

            self.info_logger.logger.log(INFO, "Getting statements from {} to {}".format(offset, offset + chunksize))
            result = self.sparql.query(offset, chunksize)
            result_info = result.info()

            if 'x-sparql-maxrows' in result_info:
                max_chunksize_server = int(result_info['x-sparql-maxrows'])

                if max_chunksize_server and max_chunksize_server < chunksize:
                    chunksize = max_chunksize_server
                    self.info_logger.logger.log(
                        INFO, "Max server rows is smaller than chunksize, new chunksize is {}".format(max_chunksize_server))

            offset = offset + chunksize

            csv_result = StringIO(result.convert().decode('utf-8'))
            data_frame = read_csv(csv_result)
            geo_data_frame = GeoDataFrame(data_frame)
            geo_data_frame[self.config.get_var_shape(self.type)] = GeoDataFrame(
                geo_data_frame[self.config.get_var_shape(self.type)].apply(lambda x: loads(x)))

            size = len(geo_data_frame)

            if results is None:
                results = geo_data_frame
            else:
                results = GeoDataFrame(concat([results, geo_data_frame]))

            if size < chunksize:
                break

        end = time.time()
        self.info_logger.logger.log(INFO, "Retrieving statements took {}s".format(round(end - start, 4)))

        self.write_cache_file(results)

        results = results.set_geometry(self.config.get_var_shape(self.type))
        results = results.set_index(self.config.get_var_uri(self.type))
        return results

    def write_cache_file(self, results):
        self.info_logger.logger.log(INFO, "Writing cache file: {}.csv".format(self.sparql.query_hash))
        results.to_csv(join('cache', '{}.csv'.format(self.sparql.query_hash)), index=False)

    def set_max_field_size_limit(self):
        dec = True
        maxInt = maxsize

        while dec:
            try:
                field_size_limit(maxInt)
                dec = False
            except OverflowError:
                maxInt = int(maxInt / 10)
