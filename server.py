#!/usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler

import json
import os
import sys

from config import load_config
from geolimes import goeLIMES

DEFAULT_PORT = 8888


class geoLIMESHandler(RequestHandler):
    def initialize(self, geolimes):
        self.geolimes = geolimes

    def post(self):
        requestHeaders = self.request.headers
        requestData = self.request.body

        if requestData and len(requestData) > 0:
            responseContentType = '*/*'
            config_json = json.loads(requestData)
            result = self.geolimes.run(config_json, False)
            responseData = json.dumps(result)

            if requestHeaders['Accept'] == None or requestHeaders['Accept'] == 'text/csv' or requestHeaders['Accept'] == '*/*':
                responseContentType = 'text/csv'
            elif requestHeaders['Accept'] == 'text/turtle':
                responseContentType = 'text/*'
            elif requestHeaders['Accept'] == 'application/json':
                responseContentType = 'application/json'
            else:
                responseContentType = 'text/html'
                responseData = 'Requested response format not supported'

        self.write(responseData)
        self.set_header('Content-Type', responseContentType)


def get_arguments():
    parser = ArgumentParser(description="Python LIMES")
    parser.add_argument("-d", "--database", type=str, dest="database_config_file", help="Path to a database config file", required=True)
    parser.add_argument("-p", "--port", type=str, dest="server_port", help="Server Port")
    parser.add_argument("-v", "--version", action="version", version="0.0.1", help="Show program version and exit")
    return parser.parse_args()


def create_app(geolimes):
    return Application([
        (r"/limes", geoLIMESHandler, {'geolimes': geolimes})
    ])


def main():
    arguments = get_arguments()
    database_config = load_config(arguments.database_config_file)
    geolimes = goeLIMES(database_config)

    if arguments.server_port != None:
        port = arguments.server_port

        if port.isdigit():
            port = int(port)
        else:
            print("The server port is not valid, using default port 8888")
            port = DEFAULT_PORT
    else:
        port = DEFAULT_PORT

    app = create_app(geolimes)
    http_server = HTTPServer(app)
    http_server.listen(port)
    print("Server started at Port {}".format(port))
    IOLoop.current().start()


if __name__ == "__main__":
    main()
