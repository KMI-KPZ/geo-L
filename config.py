#!/usr/bin/python3.6
# -*- coding: utf-8 -*-

import json


class Config:
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.config = self.load_config()
        self.check_config()

    def load_config(self):
        with open(self.config_file_path, 'r') as config_file:
            return json.loads(config_file.read())

    def check_config(self):
        if 'source' not in self.config:
            raise ConfigNotValidError("Config is missing source")

        if 'target' not in self.config:
            raise ConfigNotValidError("Config is missing target")

        if 'endpoint' not in self.config['source']:
            raise ConfigNotValidError("Config is missing source endpoint")

        if 'rawquery' not in self.config['source']:

            if 'graph' not in self.config['source']:
                raise ConfigNotValidError("Config is missing source graph")

            if 'var' not in self.config['source']:
                raise ConfigNotValidError("Config is missing source var")

            if 'property' not in self.config['source']:
                raise ConfigNotValidError("Config is missing source property")

        if 'endpoint' not in self.config['target']:
            raise ConfigNotValidError("Config is missing target endpoint")

        if 'rawquery' not in self.config['target']:

            if 'graph' not in self.config['target']:
                raise ConfigNotValidError("Config is missing target graph")

            if 'var' not in self.config['target']:
                raise ConfigNotValidError("Config is missing target var")

            if 'property' not in self.config['target']:
                raise ConfigNotValidError("Config is missing target property")

        if 'measures' not in self.config:
            raise ConfigNotValidError("No measures specified")

    def get_chunksize(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        if 'chunksize' in self.config[type]:
            return self.config[type]['chunksize']
        else:
            return -1

    def get_endpoint(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        return self.config[type]['endpoint']

    def get_geometry(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        return self.config[type]['geometry']

    def get_graph(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        return self.config[type]['graph']

    def get_limit(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        if 'limit' in self.config[type]:
            return self.config[type]['limit']
        else:
            return -1

    def get_measures(self):
        return self.config['measures']

    def get_offset(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        if 'offset' in self.config[type]:
            return self.config[type]['offset']
        else:
            return 0

    def get_prefixes(self):
        if 'prefixes' in self.config:
            return self.config['prefixes']
        else:
            return None

    def get_property(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        return self.config[type]['property']

    def get_rawquery(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        if 'rawquery' in self.config[type]:
            return self.config[type]['rawquery']
        else:
            return None

    def get_restriction(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        if 'restriction' in self.config[type]:
            return self.config[type]['restriction']
        else:
            return None

    def get_var(self, type):
        if type != 'source' and type != 'target':
            raise Exception("Wrong type (not source or target) specified")

        return self.config[type]['var']


class ConfigNotValidError(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return(self.error)
