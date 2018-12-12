#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hashlib import md5
from SPARQLWrapper import SPARQLWrapper, CSV
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError, EndPointNotFound, SPARQLWrapperException, Unauthorized

from logger import ErrorLogger

import re


class SPARQL:
    def __init__(self, config, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        self.config = config
        self.type = type
        self.query_hash = self.get_query_hash()
        self.sparql_error_logger = ErrorLogger('SparqlErrorLogger', 'sparql_errors', self.query_hash)

    def build_query(self, offset, limit=None):
        if self.config.get_rawquery(self.type) is not None:
            query = self.config.get_rawquery(self.type)
            query_offset = 'OFFSET {}'.format(offset)
            query = '{} {}'.format(query, query_offset)

            if limit is None:
                return query

            query_limit = 'LIMIT {}'.format(limit)
            return '{} {}'.format(query, query_limit)
        else:
            query_prefixes = self.build_prefixes()
            query_select = 'SELECT DISTINCT ?{} ?{}'.format(self.config.get_var_uri(self.type), self.config.get_var_shape(self.type))
            query_from = 'FROM <{}>'.format(self.config.get_graph(self.type))
            query_where = self.build_where()
            query_offset = 'OFFSET {}'.format(offset)
            query_limit = 'LIMIT {}'.format(limit)
            query = '{} {} {} {} {}'.format(query_prefixes, query_select, query_from, query_where, query_offset)

            if limit is None:
                return query

            return '{} {}'.format(query, query_limit)

    def build_prefixes(self):
        prefixes = self.config.get_prefixes()

        if prefixes is None:
            return ''

        query_prefixes = ''

        for prefix in prefixes:
            if len(query_prefixes) > 0:
                query_prefixes += ' '

            current_prefix = 'PREFIX {}: <{}>'.format(prefix['label'], prefix['namespace'])
            query_prefixes += current_prefix

        return query_prefixes

    def build_where(self):
        restriction = self.config.get_restriction(self.type)
        property = self.config.get_property(self.type)
        query_where = 'WHERE {'

        if restriction is not None:
            query_where += restriction + ' . '

        if property is not None:
            query_where += property + ' . '

        query_where += '}'

        return query_where

    def clean_query(self, query):
        query = re.sub('\n', ' ', query)
        query = re.sub('[ ]+', ' ', query)
        query = re.sub('[ ]?{[ ]?', ' {', query)
        query = re.sub('[ ]?}[ ]?', '} ', query)

        return query

    def query(self, offset, limit=None):
        sparql = SPARQLWrapper(self.config.get_endpoint(self.type))
        sparql.customHttpHeaders['Accept-Encoding'] = 'gzip'
        query = self.build_query(offset, limit)
        sparql.setQuery(query)
        sparql.setReturnFormat(CSV)

        try:
            result = sparql.query()
            return result
        except EndPointNotFound as e:
            print(e)
        except Unauthorized as e:
            print(e)
        except EndPointInternalError as e:
            print(e)
        except SPARQLWrapperException as e:
            print(e)

        return None

    def get_query_hash(self):
        query = self.clean_query(self.build_query(0))
        return md5(query.encode('utf-8')).hexdigest()
