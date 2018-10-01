#!/usr/bin/env python
# -*- coding: utf-8 -*-

from csv import field_size_limit
from geopandas import GeoDataFrame
from gzip import GzipFile
from io import StringIO, BytesIO
from logging import INFO
from numpy import arange
from os.path import join, isfile
from pandas import concat, read_csv, Series
from shapely.wkt import loads
from sys import maxsize

from logger import InfoLogger

import time


class Cache:
    def __init__(self, logger, config, sparql, type):
        self.config = config
        self.sparql = sparql
        self.type = type

        self.info_logger = logger

        self.set_max_field_size_limit()  # Needed for very long fields

    def create_cache(self):
        offset = self.config.get_offset(self.type)
        limit = self.config.get_limit(self.type)
        chunksize = self.config.get_chunksize(self.type)
        results = None

        if limit > 0 and chunksize > limit:
            chunksize = limit

        if isfile(join('cache', '{}.csv'.format(self.sparql.query_hash))):
            self.info_logger.logger.log(INFO, "Cache file {}.csv for query already exists".format(self.sparql.query_hash))
            self.info_logger.logger.log(INFO, "Loading cache file...")

            results = read_csv('cache/{}.csv'.format(self.sparql.query_hash))

            new_data = False
            max_offset = results.loc[results['offset'].idxmax()]['offset']
            more_results = self.check_more_results(max_offset + 1)

            if limit > 0 and (more_results or offset + limit < max_offset):
                offsets = Series(arange(offset, offset + limit))
            else:
                offsets = Series(arange(offset, max_offset))

            missing_offsets = offsets[~offsets.isin(results['offset'])]
            missing_offsets_diff = ~missing_offsets.diff().fillna(0).le(1)
            missing_offsets_intervals = missing_offsets.groupby(missing_offsets_diff.cumsum()).apply(lambda x: x.tolist()).tolist()

            if len(missing_offsets_intervals) > 0:
                self.info_logger.logger.log(INFO, "Cache file missing data, downloading missing data...")
                new_data = True

                for interval in missing_offsets_intervals:
                    interval_offset = interval[0]
                    limit = interval[len(interval) - 1] - interval_offset + 1
                    results = self.download_results(results, interval_offset, limit, chunksize)
                    results.sort_values(by='offset')
                    results.reset_index(drop=True)

            if limit < 0 and more_results:
                new_data = True
                data_frame = self.download_results(results, max_offset + 1, limit, chunksize)
                results = concat([results, data_frame])

            if new_data:
                self.write_cache_file(results)

        else:
            start = time.time()
            results = self.download_results(results, offset, limit, chunksize)
            end = time.time()
            self.info_logger.logger.log(INFO, "Retrieving statements took {}s".format(round(end - start, 4)))
            self.write_cache_file(results)

        if limit > 0:
            results = results.query(str(offset) + ' <= offset <= ' + str(offset + limit - 1))
        else:
            results = results.query(str(offset) + ' <= offset')

        results = GeoDataFrame(results)
        results[self.config.get_var_shape(self.type)] = GeoDataFrame(results[self.config.get_var_shape(self.type)].apply(lambda x: loads(x)))
        results = results.set_geometry(self.config.get_var_shape(self.type))
        results = results.set_index(self.config.get_var_uri(self.type))
        return results

    def download_results(self, results, offset, limit, chunksize):
        start_offset = offset
        run = True

        while(run):
            current_max = offset + chunksize

            if limit > 0 and offset + chunksize >= start_offset + limit:
                chunksize = start_offset + limit - offset
                current_max = start_offset + limit
                run = False

            self.info_logger.logger.log(INFO, "Getting statements from {} to {}".format(offset, current_max))
            result = self.sparql.query(offset, chunksize)
            result_info = result.info()

            if 'x-sparql-maxrows' in result_info:
                max_chunksize_server = int(result_info['x-sparql-maxrows'])

                if max_chunksize_server and max_chunksize_server < chunksize:
                    chunksize = max_chunksize_server
                    self.info_logger.logger.log(
                        INFO, "Max server rows is smaller than chunksize, new chunksize is {}".format(max_chunksize_server))

            if 'content-encoding' in result_info and result_info['content-encoding'] == 'gzip':
                csv_result = self.gunzip_response(result)
            else:
                csv_result = StringIO(result.convert().decode('utf-8'))

            result.response.close()
            data_frame = read_csv(csv_result)
            data_frame['offset'] = range(offset, offset + len(data_frame))
            size = len(data_frame)

            if results is None:
                results = data_frame
            else:
                results = concat([results, data_frame])

            if size < chunksize:
                break

            offset = offset + chunksize

        return results

    def check_more_results(self, offset):
        result = self.sparql.query(offset, 1)
        result_info = result.info()

        if 'content-encoding' in result_info and result_info['content-encoding'] == 'gzip':
            csv_result = self.gunzip_response(result)
        else:
            csv_result = StringIO(result.convert().decode('utf-8'))

        data_frame = read_csv(csv_result)
        return len(data_frame) == 1

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

    def gunzip_response(self, response):
        buffer = BytesIO()
        buffer.write(response.response.read())
        buffer.seek(0)

        with GzipFile(fileobj=buffer, mode='rb') as unzipped:
            result = unzipped.read()
            return StringIO(result.decode('utf-8'))
