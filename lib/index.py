#!/usr/bin/env python

import psycopg2
import numpy as np
import scikits.ann as ann


IDS_PER_CHUNK            = 2000
KDTREE_DIM               = 6 
NUM_TRACKS_PER_TRACKLIST = KDTREE_DIM - 1
NUM_TRACKS_SCALE_FACTOR  = 10000
SECTORS_PER_SECOND       = 75

class CDLookup(object):
    '''
    Load the track durations from the database and create a ANN kdtree in order to
    find nearest neighbors in "CD Table of contents" space 
    '''

    def __init__(self, host, database, user, passwd):
        self.host = host
        self.database = database
        self.user = user
        self.passwd = passwd

    def validate_toc(self, toc):
        '''Ensure that the passed toc is valid'''

        first, num_tracks, leadout, offsets = toc.split(' ', 3)
        num_tracks = int(num_tracks)
        if int(first) != 1: return False
        if num_tracks < 1 or num_tracks > MAX_NUM_TRACKS: return False

        offsets = offsets.split()
        for offset in offsets:
            if offset >= leadout: return False

        if len(offsets) != num_tracks: return False

        return True

    def convert_toc_to_durations(self, toc):
        '''Convert a sector based toc to a simple array of durations in milliseconds'''

        first, num_tracks, leadout, offsets = toc.split(' ', 3)
        num_tracks = int(num_tracks)
        offsets = offsets.split()
        offsets.append(leadout)

        durations = []
        for i in xrange(len(offsets) - 1):
            durations.append((offsets[i + 1] - offsets[i]) * SECTORS_PER_SECOND)

        print toc
        print durations
        return durations

    def select_tracks(self, tracks):
        '''
        Deterministically pick tracks from an array of tracks. Since tracklists with
        more than NUM_TRACKS_PER_TRACKLIST can't all fit into the kdTree, we have to
        pick at max NUM_TRACKS_PER_TRACKLIST tracks.
        Warning: The argument tracks may have zeros appended to it to fit the point size
        '''

        # The first dimension will be the number of tracks on a given tracklist  
        # This ensures that only tracklists with the same number of tracks would
        # ever match
        point = [len(tracks) * NUM_TRACKS_SCALE_FACTOR]

        # If there are the right number of tracks or fewer, use them all. zero pad if necessary
        if len(tracks) <= NUM_TRACKS_PER_TRACKLIST:
            point.extend(tracks)
            point.extend([0] * (NUM_TRACKS_PER_TRACKLIST - len(tracks)))
            return point

        # Setup the point array and pick the first and last track
        point.extend([0] * NUM_TRACKS_PER_TRACKLIST)
        point[1] = tracks[0]
        point[KDTREE_DIM - 1] = tracks[len(tracks) - 1]

        # Now pick the remaining tracks as evenly spaced across the list as possible
        spread = int((len(tracks) - 2) / (float)(KDTREE_DIM - 3));
        for dest in xrange(0, KDTREE_DIM-3):
            src = int(spread * (dest + 1))
            point[dest + 2] = tracks[src]

        return point

    def lookup(self, kdtree, toc):
        '''Carry out a lookup of a toc'''
        point = self.select_tracks(self.convert_toc_to_durations(toc))
        ret = k.knn(point, 1)

    def load_data_from_db(self):
        '''
        Create an ANN kdtree from the track durations in the DB
        '''

        # Connect to the DB
        try:
            conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (self.database, self.user, self.host, self.passwd))
        except psycopg2.OperationalError, msg:
            print "Cannot connect to the database: %s" % unicode(msg)
            return False

        # Get the number of rows we need to process
        curs = conn.cursor()

        # TODO: This overestimates!!
        curs.execute("SELECT count(*) FROM musicbrainz.tracklist")
        rows = curs.fetchall()
        totalRows = rows[0][0]
        if not totalRows: return False

        points = np.empty((totalRows, KDTREE_DIM))

        rowsProcessed = 0
        numChunks = (totalRows / IDS_PER_CHUNK) + 1

        # For debugging
        numChunks = 10
        for i in xrange(numChunks):
            curs.execute("""SELECT t.tracklist, r.length 
                              FROM musicbrainz.track t, musicbrainz.recording r 
                             WHERE t.recording = r.id 
                               AND t.tracklist >= %d 
                               AND t.tracklist < %d
                          ORDER BY tracklist, position""" %( i * IDS_PER_CHUNK, (i + 1) * IDS_PER_CHUNK))

            rows = curs.fetchall()

            curTracklist = 0
            durations = []
            invalid = False
            for row in rows:
                if curTracklist > 0 and row[0] != curTracklist:
                    if not invalid:
                        point = self.select_tracks(durations)
                        np.put(points[rowsProcessed], xrange(0, KDTREE_DIM), point)
                        rowsProcessed += 1
                    durations = []
                    invalid = False

                if row[1] == 0: invalid = True;
                durations.append(row[1])
                curTracklist = row[0]

            print "%d of %d (%d%% done)" % (rowsProcessed, totalRows, int(100 * rowsProcessed / totalRows))

        conn.close()

        print "processed %d rows." % rowsProcessed
        print "build tree"

        return ann.kdtree(points)

cd = CDLookup("localhost", "musicbrainz_db_ngs", "musicbrainz_user", "")
k = cd.load_data_from_db()

print k.knn([90000, 318970, 261543, 364316, 254706, 397226], 1)
print k.knn([120000, 150906, 214733, 192960, 232266, 170000], 1)

