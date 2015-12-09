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
from icarus.models.space_saving import SpaceSavingCache

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

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertEquals([contents, cache_hits], [60000, 38995])


    def test_dsca_fastly(self):
        import csv
        c = []
        for window_size in [1500, 3000, 6000, 9000, 12000, 15000, 21000]:
            c.append(DataStreamCachingAlgorithmCache(100, monitored=500, window_size=window_size))
        cache_hits = [0]*7
        contents = 0

        with open('../../../resources/Fastly_traces/requests_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                for i, cache in enumerate(c):
                    if cache.get(content):
                        cache_hits[i] += 1
                    else:
                        cache.put(content)

                if contents % 100000 == 0:
                    print contents, 14885146, float(contents)/float(14885146)

        self.assertEquals(cache_hits, [2205377, 2148139, 2101392, 2054625, 2021313, 1990615, 1933566])


    def test_small_sliding_window(self):
        c = DataStreamCachingAlgorithmCache(100, monitored=500, window_size=1500)
        cache_hits = 0
        contents = 0
        input_stream = [1,2,1,3,1,2,4,3,5,5,6,1,7,4,2,6,1,1,4,5,1,7,8,8,8,4,6,6,4,1,6,4,8,1,8,8,9,6,1,4]
        window_cache = SpaceSavingCache(5, monitored=5)
        cumulative_cache = SpaceSavingCache(5, monitored=5)
        window_size = 20

        for i, input_element in enumerate(input_stream, start=1):
            cumulative_cache.put(input_element)
            window_cache.put(input_element)

            if i % window_size == 0:
                print 'window cache:'
                window_cache.print_buckets()
                window_cache.clear()
                print 'cumulative cache:'
                cumulative_cache.print_buckets()
                print ''


