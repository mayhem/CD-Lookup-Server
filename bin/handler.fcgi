#!/usr/bin/env python

from flup.server.fcgi_fork import WSGIServer; 

sys.path.append(os.environ["LIBDIR"])

WSGIServer(search, 
           bindAddress = '/tmp/cdlookup.fcgi.sock', 
           maxRequests = 100, 
           minSpare = 5, 
           maxSpare = 10, 
           maxChildren = 20).run() 
