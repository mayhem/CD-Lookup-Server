#!/usr/bin/env python

import cgi
import sys
import cherrypy
from index import CDLookupIndex

# How many milliseconds two CDs can be apart and still be considered a match
DISTANCE_THRESHOLD = 30000

cdlookup_index = None

def bad_request(start_response, msg):
    '''Return a bad request response to the caller'''
    start_response('403 BAD REQUEST', [('Content-Type', 'text/plain')])
    return msg 

class CDLookupServer(object):

    @cherrypy.expose
    def index(self):
        return "<html><body>This is the CD Lookup server.</body></html>\n";

    @cherrypy.expose
    def default(self, ws, ver, resource, toc, dist = DISTANCE_THRESHOLD):

        if cherrypy.request.method != 'GET':
            raise cherrypy.HTTPError(400, "Only GET method is supported")

        if ws != 'ws' or ver != '1' or resource != 'toc':
            raise cherrypy.HTTPError(404, "Not found. Only the /ws/1/toc resource is available.")

        # Remove + from the toc
        toc = toc.replace('+', ' ')
        if not cdlookup_index.validate_toc(toc):
            raise cherrypy.HTTPError(400, "Invalid toc.")

        # check distance
        try:
            dist = int(dist)
            if dist < 0: raise ValueError
        except ValueError:
            raise cherrypy.HTTPError(400, "The dist parameter must be a positive int value.\n")

        cherrypy.response.headers["Content-Type"] = "application/json"
        return cdlookup_index.lookup(toc, dist)

# create the lookup index
cdlookup_index = CDLookupIndex("localhost", "musicbrainz_db_ngs", "musicbrainz_user", "")

# Run a development server
cherrypy.quickstart(CDLookupServer())
