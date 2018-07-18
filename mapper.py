#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

from ctypes import c_char_p
from multiprocessing import cpu_count, Array
from multiprocessing.pool import Pool
from os.path import join
from shapely.errors import TopologicalError
from shapely.geometry import shape
from shapely.wkt import loads

from cache import FastRetree
from logger import ResultLogger

import fiona
import time


class Mapper:
    def __init__(self, config, source_sparql, target_sparql, source, target):
        self.config = config
        self.source_sparql = source_sparql
        self.target_sparql = target_sparql
        self.source = source
        self.target = target
        self.measures = config.get_measures()
        self.target_index = FastRetree(join('cache', self.target_sparql.get_query_hash()))

        # Shared data necessary for RAM usage
        global target_data
        target_data = Array(c_char_p, len(self.target), lock=False)
        target_data[:len(self.target)] = self.target

        self.result_logger = ResultLogger('ResultLogger', source_sparql.get_query_hash(), target_sparql.get_query_hash())

    def map(self):
        start = time.time()
        count = 0
        cpus = cpu_count()

        if cpus <= 0:
            cpus = 2

        print("Mapping started...")
        # TODO Add check for length and cpu_count() to set chunksize accordingly (Check min chunksize)
        chunks = [self.source[x:x + 10000] for x in range(0, len(self.source), 10000)]
        args = []
        results = []

        with Pool(processes=cpus, maxtasksperchild=1000) as pool:
            for chunk in chunks:
                args.append((self.measures, chunk, self.target_index))

            results = pool.starmap(async_map, args)

        for chunk in results:
            if len(chunk) > 0:
                for item in chunk:
                    self.result_logger.logger.info(item)
                    count += 1

        self.target_index.close()

        end = time.time()
        print("Mapping took : {}s".format(round(end - start, 4)))
        print("{} mappings found".format(count))


def async_map(measures, source_shapes, target_index):
    results = []

    for source_shape in source_shapes:
        chunk_results = compare(measures, source_shape, target_index)

        if len(chunk_results) > 0:
            results += chunk_results

    return results


def compare(measures, source_item, target_index):
    results = []
    item = source_item.decode('utf-8')
    item = item.split('","')
    uri = item[0][1:]
    shape = item[1][:-2]
    geometry = loads(shape)

    for j in target_index.intersection(geometry.coords[0]):
        target_item = target_data[j]
        target_item = target_item.decode('utf-8')
        target_item = target_item.split('","')
        target_uri = target_item[0][1:]
        target_shape = target_item[1][:-2]
        target_geometry = loads(target_shape)

        try:
            if 'contains' in measures and target_geometry.contains(item):
                results.append('"{}", "contains", "{}"'.format(uri, target_uri))

            if 'crosses' in measures and item.crosses(target_geometry):
                results.append('"{}", "crosses", "{}"'.format(uri, target_uri))

            if 'equals' in measures and item.equals(target_geometry):
                results.append('"{}", "equals", "{}"'.format(uri, target_uri))

            if 'overlaps' in measures and item.overlaps(target_geometry):
                results.append('"{}", "overlaps", "{}"'.format(uri, target_uri))

            if 'touches' in measures and item.touches(target_geometry):
                results.append('"{}", "touches", "{}"'.format(uri, target_uri))

            if 'within' in measures and geometry.within(target_geometry):
                results.append('"{}", "within", "{}"'.format(uri, target_uri))
        except TopologicalError as e:
            print(e)

    return results
