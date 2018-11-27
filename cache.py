#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gzip import GzipFile
from io import BytesIO, StringIO
from logging import INFO
from more_itertools import consecutive_groups
from pandas import DataFrame, read_csv

import psycopg2
import time

# TODO: Escape data taken from config file


class Cache:
    def __init__(self, logger, config, sparql, type):
        self.config = config
        self.sparql = sparql
        self.type = type

        self.info_logger = logger

    def create_cache(self):
        start = time.time()
        offset = self.config.get_offset(self.type)
        limit = self.config.get_limit(self.type)
        chunksize = self.config.get_chunksize(self.type)

        connection = psycopg2.connect(self.config.get_database_string())
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS {}({} VARCHAR, {} VARCHAR, server_offset BIGINT, geo GEOMETRY)".format(
            'public.table_' + self.sparql.query_hash, self.config.get_var_uri(self.type), self.config.get_var_shape(self.type)))
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_geo_{} ON {} USING GIST(geo);".format(
            self.sparql.query_hash, 'table_' + self.sparql.query_hash))
        connection.commit()
        cursor.close()

        self.info_logger.logger.log(INFO, "Checking {} cache".format(self.type))

        new_data = False
        min_offset = offset
        min_server_offset, max_server_offset = self.find_min_max_server_offset(connection)

        if limit > 0:
            max_offset = offset + limit - 1

            if (min_server_offset > 0 and max_offset < min_server_offset) or (max_server_offset == 0):
                self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")
                self.download_results(connection, offset, limit, chunksize)
                new_data = True
            elif min_offset > max_server_offset:
                more_results = self.check_more_results(min_offset + 1)

                if more_results:
                    self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")
                    self.download_results(connection, offset, limit, chunksize)
                    new_data = True
            else:
                intervals = []

                if offset < min_server_offset:
                    intervals.append((offset, min_server_offset - 1))
                    min_offset = min_server_offset

                if max_offset > max_server_offset:
                    intervals.append((max_server_offset + 1, max_offset))
                    max_offset = max_server_offset

                missing_limit = max_offset - min_offset + 1

                missing_intervals = self.find_missing_data(connection, min_offset, missing_limit)
                missing_intervals = sorted(missing_intervals + intervals)

                if len(missing_intervals) > 0:
                    self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")

                    for interval in missing_intervals:
                        interval_offset = interval[0]
                        interval_limit = 1

                        if len(interval) > 1:
                            interval_limit = interval[1] - interval_offset + 1

                        self.download_results(connection, interval_offset, interval_limit, chunksize)

                    new_data = True

                self.info_logger.logger.log(INFO, "Data already cached..")
        else:
            if max_server_offset == 0 or min_offset > max_server_offset:
                self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")
                self.download_results(connection, offset, limit, chunksize)
                new_data = True
            elif min_offset < min_server_offset:
                missing_interval = [(min_offset, min_server_offset - 1)]
                missing_intervals = self.find_missing_data(connection, min_offset, missing_limit)
                missing_intervals = missing_interval + missing_intervals
                more_results = self.check_more_results(min_offset + 1)

                if more_results:
                    missing_intervals + [(max_server_offset + 1,)]

                self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")

                for interval in missing_intervals:
                    interval_offset = interval[0]
                    interval_limit = -1

                    if len(interval) > 1:
                        interval_limit = interval[1] - interval_offset + 1

                    self.download_results(connection, interval_offset, interval_limit, chunksize)

                new_data = True
            else:
                missing_intervals = self.find_missing_data(connection, min_offset, max_server_offset)
                more_results = self.check_more_results(min_offset + 1)

                if more_results:
                    missing_intervals += [(max_server_offset + 1,)]

                if len(missing_intervals) > 0:
                    self.info_logger.logger.log(INFO, "Cache is missing data, downloading missing data...")

                    for interval in missing_intervals:
                        interval_offset = interval[0]
                        interval_limit = -1

                        if len(interval) > 1:
                            interval_limit = interval[1] - interval_offset + 1

                        self.download_results(connection, interval_offset, interval_limit, chunksize)

                    new_data = True

        if new_data:
            # TODO write log file with invalid geometries
            end = time.time()
            self.info_logger.logger.log(INFO, "Retrieving statements took {}s".format(round(end - start, 4)))

        invalid_geometries_count = self.count_invalid_geometries(connection)
        self.info_logger.logger.log(INFO, "{} invalid geometries in {}".format(invalid_geometries_count, self.type))

        connection.close()

    def download_results(self, connection, offset, limit, chunksize):
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
            size = self.insert(connection, csv_result, offset, chunksize)

            if size < chunksize:
                break

            offset = offset + chunksize

    def insert(self, connection, result, offset, chunksize):
        data_frame = read_csv(result)
        data_frame['server_offset'] = range(offset, offset + len(data_frame))
        data_frame_column_headers = list(data_frame)
        column_headers = [self.config.get_var_uri(self.type), self.config.get_var_shape(self.type), 'server_offset']

        for data_frame_column_header in data_frame_column_headers:
            if data_frame_column_header not in column_headers:
                data_frame.drop(data_frame_column_header, 1, inplace=True)

        size = len(data_frame)

        output = StringIO()
        data_frame.to_csv(output, sep=';', index=False)
        output.seek(0)

        cursor = connection.cursor()
        cursor.copy_expert(sql="COPY {} ({}, {}, {}) FROM STDIN WITH CSV HEADER DELIMITER AS ';'".format(
            'table_' + self.sparql.query_hash, self.config.get_var_uri(self.type), self.config.get_var_shape(self.type), 'server_offset'),
            file=output)
        cursor.execute("UPDATE {} SET geo = ST_GeomFromText({}) WHERE geo IS NULL AND ST_ISVALID(ST_GeomFromText({}))".format(
            'table_' + self.sparql.query_hash, self.config.get_var_shape(self.type), self.config.get_var_shape(self.type)))
        connection.commit()
        cursor.close()

        return size

    def gunzip_response(self, response):
        buffer = BytesIO()
        buffer.write(response.response.read())
        buffer.seek(0)

        with GzipFile(fileobj=buffer, mode='rb') as unzipped:
            result = unzipped.read()
            return StringIO(result.decode('utf-8'))

    def find_min_max_server_offset(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT MIN(server_offset), MAX(server_offset) FROM {}".format('table_' + self.sparql.query_hash))
        result = cursor.fetchone()
        cursor.close()

        min_offset = 0
        max_offset = 0

        if result[0] != None:
            min_offset = result[0]

        if result[1] != None:
            max_offset = result[1]

        return min_offset, max_offset

    def find_missing_data(self, connection, offset, limit):
        cursor = connection.cursor()
        cursor.execute("""
        SELECT s.i AS missing 
        FROM generate_series({}, {}) s(i) 
        WHERE NOT EXISTS (
            SELECT 1 FROM {} WHERE server_offset = s.i 
        ) 
        ORDER BY missing
        """.format(offset, offset + limit - 1, 'table_' + self.sparql.query_hash))
        result = cursor.fetchall()
        cursor.close()

        missing_values = [x[0] for x in result]
        missing_intervals = list(self.find_ranges(missing_values))

        return missing_intervals

    def find_ranges(self, iterable):
        for group in consecutive_groups(iterable):
            group = list(group)
            if len(group) == 1:
                yield group[0]
            else:
                yield group[0], group[-1]

    def check_more_results(self, offset):
        result = self.sparql.query(offset, 1)
        result_info = result.info()

        if 'content-encoding' in result_info and result_info['content-encoding'] == 'gzip':
            csv_result = self.gunzip_response(result)
        else:
            csv_result = StringIO(result.convert().decode('utf-8'))

        data_frame = read_csv(csv_result)
        return len(data_frame) == 1

    def count_invalid_geometries(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) AS count FROM {} WHERE geo IS NULL".format('table_' + self.sparql.query_hash))
        result = cursor.fetchone()
        cursor.close()

        return result[0]
