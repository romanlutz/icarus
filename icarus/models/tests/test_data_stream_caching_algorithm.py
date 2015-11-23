from __future__ import division

__author__ = 'romanlutz'


import sys
import random
if sys.version_info[:2] >= (2, 7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The unittest2 package is needed to run the tests.")
del sys

from icarus.models.data_stream_caching_algorithm import DataStreamCachingAlgorithmCache

class TestStreamSummary(unittest.TestCase):

    def test_short_detail(self):
        dsca = DataStreamCachingAlgorithmCache(5, monitored=7, window_size=50)

        for i in range(1, 101):
            print 'iteration:', i
            req = random.randint(1, random.randint(1, 20))
            print 'request:', req
            if not dsca.get(req):
                dsca.put(req)
            dsca.print_caches()
            print ''

            if i % dsca._window_size == 0:
                print 'END OF WINDOW'
                print ''

    def test_long(self):
        dsca = DataStreamCachingAlgorithmCache(20, monitored=100, window_size=10000)

        for i in range(1, 10*dsca._window_size):
            req = random.randint(1, random.randint(1, 150))
            if not dsca.get(req):
                dsca.put(req)

            if i % dsca._window_size == dsca._window_size - 1:
                print ''
                dsca.print_caches()
                print ''
            if i % dsca._window_size == 0:
                print 'END OF WINDOW'
                print ''
                dsca.print_caches()
                print ''


    def test_dsca_ibm(self):
        import csv
        c = DataStreamCachingAlgorithmCache(100, monitored=500, window_size=1500)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/requests_full_ibm.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[0])

                if contents % 1500 == 0:
                    print 'number of requests so far: ', contents, 'next:', content
                    c.print_caches()
                    print ''
                    print c._ss_cache.guaranteed_top_k()

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

                if contents % 1500 == 0:
                    print 'cache after 1500*k transformation'
                    c.print_caches()
                    print ''

        self.assertEquals([contents, cache_hits], [60000, 38780])
