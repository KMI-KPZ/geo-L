#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os.path import join
from time import time

import logging


class ErrorLogger:
    def __init__(self,  name, error_type, query_hash):
        self.logger = logging.getLogger('{}_{}'.format(query_hash, name))
        self.logger.setLevel(logging.ERROR)
        self.logger.propagate = False

        logger_handler = logging.FileHandler(join('logs', '{}_{}.log'.format(query_hash, error_type)))
        logger_handler.setLevel(logging.ERROR)
        logger_formater = logging.Formatter('%(message)s')
        logger_handler.setFormatter(logger_formater)
        self.logger.addHandler(logger_handler)


class ResultLogger:
    def __init__(self, name, source_hash, target_hash):
        self.logger = logging.getLogger('{}_{}_{}'.format(source_hash, target_hash, name))
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        logger_handler = logging.FileHandler(join('output', '{}_{}.log'.format(source_hash, target_hash)), mode='w')
        logger_handler.setLevel(logging.INFO)
        logger_formater = logging.Formatter('%(message)s')
        logger_handler.setFormatter(logger_formater)
        self.logger.addHandler(logger_handler)


class InfoLogger:
    def __init__(self, name, query_hash):
        self.logger = logging.getLogger('{}_{}_{}'.format(name, query_hash, time()))
        self.logger.setLevel(logging.INFO)

        logger_formater = logging.Formatter('%(asctime)s - %(message)s')
        logger_handler = logging.StreamHandler()
        logger_handler.setLevel(logging.INFO)
        logger_handler.setFormatter(logger_formater)
        self.logger.addHandler(logger_handler)

        logger_handler = logging.FileHandler(join('logs', '{}.log'.format(query_hash)))
        logger_handler.setLevel(logging.INFO)
        logger_handler.setFormatter(logger_formater)
        self.logger.addHandler(logger_handler)


def load_logfile(query_hash, error_type):
    try:
        with open(join('logs', '{}_{}.log'.format(query_hash, error_type))) as logfile:
            content = logfile.readlines()
            return [line.strip() for line in content]
    except:
        return []
