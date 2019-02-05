#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.web import Application, RequestHandler

import json
import os
import sys

import main as limes

define('port', default=8888, help='Server port')


class geoLIMESHandler(RequestHandler):
    def post(self):
        requestHeaders = self.request.headers
        requestData = self.request.body

        if requestData and len(requestData) > 0:
            responseContentType = '*/*'
            result = limes.run(requestData, False)
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


def create_app():
    return Application([
        (r"/limes", geoLIMESHandler)
    ])


def main():
    app = create_app()
    http_server = HTTPServer(app)
    http_server.listen(options.port)
    print("Server started")
    IOLoop.current().start()


if __name__ == "__main__":
    main()
