#!/usr/bin/env python

from wsgiref.simple_server import make_server
import cgi
import sys
from cgi import FieldStorage;

MAX_NUM_TRACKS   = 99
DEFAULT_DISTANCE = 0.0


def bad_request(start_response, msg):
    '''Return a bad request response to the caller'''
    start_response('403 BAD REQUEST', [('Content-Type', 'text/plain')])
    return msg 

def lookup(start_response, toc, distance):
    '''Carry out a lookup'''

    start_response('200 OK', [('Content-Type', 'text/plain')])
    return ['[]\n']

def cdlookup_server(environ, start_response):
    """WSGI server application for cdlookup server. This handler validates requests and carries out lookups"""

    if environ['REQUEST_METHOD'].upper() != 'GET':
        return bad_request(start_response, "Only GET method accepted\n")

    url = environ["PATH_INFO"]
    if not url.startswith("/ws/1/toc"): 
        return bad_request(start_response, "Invalid URL requested. Only /ws/1/toc/<toc> is supported.")
    toc = url[10:].replace("+", " ")

    args = FieldStorage(environ=environ)
    if not validate_toc(toc):
        return bad_request(start_response, "Invalid toc passed.")

    try:
        distance = args['distance']
        if distance < 0.0:
            return bad_request(start_response, "The distance parameter must be a positive float value.")
    except KeyError:
        distance = DEFAULT_DISTANCE

    return lookup(start_response, toc, distance)

# Run a development server
httpd = make_server('localhost', 8000, cdlookup_server);
while(True):
    httpd.handle_request()
