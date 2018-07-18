#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

from fiona import collection
from os.path import join, isfile
from rtree import Rtree
from rtree.index import Index
from shapely.geometry import mapping, shape
from shapely.wkt import loads

import pickle
import time


class Cache:
    def __init__(self, config, sparql, type):
        self.config = config
        self.sparql = sparql
        self.type = type

    def create_cache_chunks(self, index=False):
        if isfile(join('cache', '{}.bin'.format(self.sparql.query_hash))):
            print("Cache file {}.bin for query already exists".format(self.sparql.query_hash))
            print("Loading cache file...")
            with open('cache/{}.bin'.format(self.sparql.query_hash), 'rb') as cache_file:
                items = cache_file.readlines()

                if index:
                    if not isfile(join('cache', '{}.idx'.format(self.sparql.query_hash))):
                        self.write_index_file(items)

                return items

        offset = self.config.get_offset(self.type)
        limit = self.config.get_limit(self.type)
        chunksize = self.config.get_chunksize(self.type)
        results = []
        run = True

        start = time.time()

        while(run):
            if limit > 0 and offset + chunksize >= limit:
                chunksize = limit - offset
                run = False

            print("Getting statements from {} to {}".format(offset, offset + chunksize))
            result = self.sparql.query(offset, chunksize)
            result_info = result.info()

            if 'x-sparql-maxrows' in result_info:
                max_chunksize_server = int(result_info['x-sparql-maxrows'])

                if max_chunksize_server and max_chunksize_server < chunksize:
                    chunksize = max_chunksize_server
                    print("Max server rows is smaller than chunksize, new chunksize is {}".format(max_chunksize_server))

            offset = offset + chunksize

            for idx, item in enumerate(result):
                if idx > 0:
                    results.append(item)

        end = time.time()
        print("Retrieving statements took {}".format(round(end - start, 4)))

        self.write_cache_file(results)

        if index:
            self.write_index_file(results)

        return results

    def write_cache_file(self, results):
        print("Writing cache file: {}.bin".format(self.sparql.query_hash))

        with open(join('cache', '{}.bin'.format(self.sparql.query_hash)), 'wb') as cache_file:
            cache_file.writelines(results)

    def write_index_file(self, results):
        print("Writing index file for {}".format(self.sparql.query_hash))

        idx = FastRetree(join('cache', self.sparql.query_hash), rtree_generator(results))
        idx.close()


class FastRetree(Rtree):
    def dumps(self, obj):
        return pickle.dumps(obj, -1)


def rtree_generator(items):
    for i, item in enumerate(items):
        item_decoded = item.decode('utf-8')
        item_split = item_decoded.split('","')
        uri = item_split[0][1:]
        shape = item_split[1][:-2]
        geometry = loads(shape)

        if not geometry.is_empty:  # Exclude empty geometry
            yield (i, geometry.bounds, None)
