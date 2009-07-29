#!/usr/bin/env python

import cgi
import sys
from cgi import FieldStorage;
from werkzeug import run_simple
from index import CDLookupIndex

DISTANCE_THRESHOLD = 10000

cdlookup_index = None

def bad_request(start_response, msg):
    '''Return a bad request response to the caller'''
    start_response('403 BAD REQUEST', [('Content-Type', 'text/plain')])
    return msg 

def cdlookup_server(environ, start_response):
    """WSGI server application for cdlookup server. This handler validates requests and carries out lookups"""

    global cdlookup_index

    if environ['REQUEST_METHOD'].upper() != 'GET':
        return bad_request(start_response, "Only GET method accepted\n")

    url = environ["PATH_INFO"]
    if not url.startswith("/ws/1/toc"): 
        return bad_request(start_response, "Invalid URL requested. Only /ws/1/toc/<toc> is supported.\n")
    toc = url[10:].replace("+", " ")

    args = FieldStorage(environ=environ)
    if not cdlookup_index.validate_toc(toc):
        return bad_request(start_response, "Invalid toc passed.\n")

    try:
        distance = int(args.getvalue('dist', 0))
        print distance
        if distance < 0.0:
            return bad_request(start_response, "The distance parameter must be a positive int value.\n")
    except KeyError:
        distance = DISTANCE_THRESHOLD

    start_response('200 OK', [('Content-Type', 'text/plain')])
    return cdlookup_index.lookup(toc, distance)

# create the lookup index
cdlookup_index = CDLookupIndex("localhost", "musicbrainz_db_ngs", "musicbrainz_user", "")

# Run a development server
run_simple('localhost', 8000, cdlookup_server, use_reloader=True, threaded=True, processes = 0);
