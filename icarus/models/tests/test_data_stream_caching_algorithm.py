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

from icarus.models.data_stream_caching_algorithm import DataStreamCachingAlgorithmCache, \
    DataStreamCachingAlgorithmWithSlidingWindowCache, AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache
import pprint

pp = pprint.PrettyPrinter(indent=4)


class TestDSCA(unittest.TestCase):
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

                if (contents - 1) % 1500 == 0:
                    # assert that end of window operation was executed by checking for SS cache size of 0
                    self.assertEquals(c._ss_cache._cache.size, 0)

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertEquals([contents, cache_hits], [60000, 38808])

    def test_dsca_fastly(self):
        import csv
        c = []
        for window_size in [1500, 3000, 6000, 9000, 12000, 15000, 21000]:
            c.append(DataStreamCachingAlgorithmCache(100, monitored=500, window_size=window_size))
        cache_hits = [0] * 7
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
                    print contents, 14885146, float(contents) / float(14885146)

        self.assertEquals(cache_hits, [2205377, 2148139, 2101392, 2054625, 2021313, 1990615, 1933566])

    def test_small_sliding_window(self):
        cache_hits = 0
        contents = 0

        input_stream = [1, 2, 1, 3, 1, 2, 4, 3, 5, 5, 6, 1, 7, 4, 2, 6, 1, 1, 4, 5,
                        1, 7, 8, 8, 8, 4, 6, 6, 4, 1, 6, 4, 8, 1, 8, 8, 9, 6, 1, 4]

        c = DataStreamCachingAlgorithmWithSlidingWindowCache(5, monitored=5, subwindows=2, subwindow_size=20)
        for i, input_element in enumerate(input_stream, start=1):

            contents += 1
            if not c.get(input_element):
                c.put(input_element)
            else:
                cache_hits += 1

            if i == 20:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 6)
                self.assertEquals(stream_summary.bucket_map[6][0].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 4)
                self.assertEquals(stream_summary.bucket_map[4][1].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[5], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[6], 3)
                self.assertEquals(stream_summary.bucket_map[3][1].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[2], 3)
                self.assertEquals(stream_summary.bucket_map[3][0].max_error, 2)

            if i == 40:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[8], 6)
                self.assertEquals(stream_summary.bucket_map[6][0].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[6], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 1)
                self.assertEquals(stream_summary.id_to_bucket_map[1], 4)
                self.assertEquals(stream_summary.bucket_map[4][1].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 2)
                self.assertEquals(stream_summary.bucket_map[2][0].max_error, 1)

        self.assertEquals([cache_hits, contents], [25, 40])

    def test_medium_sliding_window(self):
        cache_hits = 0
        contents = 0

        input_stream = [13, 2, 1, 6, 7, 2, 3, 6, 9, 8, 10, 1, 4, 6, 6, 1, 13, 3, 1, 5,
                        1, 15, 2, 17, 3, 6, 1, 14, 2, 1, 5, 17, 6, 3, 9, 7, 3, 16, 8, 2,
                        19, 4, 1, 4, 6, 6, 1, 5, 14, 8, 2, 9, 14, 1, 6, 4, 3, 7, 1, 9,
                        2, 1, 2, 4, 1, 4, 5, 11, 4, 4, 1, 17, 6, 1, 13, 9, 3, 13, 3, 1,
                        14, 4, 2, 3, 9, 8, 2, 7, 8, 14, 6, 3, 3, 2, 2, 1, 1, 7, 4, 3]

        c = DataStreamCachingAlgorithmWithSlidingWindowCache(5, monitored=5, subwindows=2, subwindow_size=20)
        for i, input_element in enumerate(input_stream, start=1):

            contents += 1
            if not c.get(input_element):
                c.put(input_element)
                hit = False
            else:
                cache_hits += 1
                hit = True

            if i == 20:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[6], 4)
                self.assertEquals(stream_summary.bucket_map[4][2].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[5], 4)
                self.assertEquals(stream_summary.bucket_map[4][1].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[13], 3)
                self.assertEquals(stream_summary.bucket_map[3][0].max_error, 2)
                self.assertEquals(c._lru_cache.dump(), [5, 1, 3, 13, 6])
                self.assertEquals(c._guaranteed_top_k, [])

            if i == 40:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[2], 5)
                self.assertEquals(stream_summary.bucket_map[5][2].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[8], 5)
                self.assertEquals(stream_summary.bucket_map[5][1].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[16], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[1], 3)
                self.assertEquals(stream_summary.bucket_map[3][0].max_error, 0)
                self.assertEquals(c._lru_cache.dump(), [2, 8, 16, 3, 7])
                self.assertEquals(c._guaranteed_top_k, [])

            if i == 60:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 6)
                self.assertEquals(stream_summary.bucket_map[6][1].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 6)
                self.assertEquals(stream_summary.bucket_map[6][0].max_error, 5)
                self.assertEquals(stream_summary.id_to_bucket_map[6], 5)
                self.assertEquals(stream_summary.bucket_map[5][1].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[7], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 3)

            if i == 80:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 5)
                self.assertEquals(stream_summary.bucket_map[5][3].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 5)
                self.assertEquals(stream_summary.bucket_map[5][2].max_error, 1)
                self.assertEquals(stream_summary.id_to_bucket_map[13], 5)
                self.assertEquals(stream_summary.bucket_map[5][1].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 2)
                self.assertEquals(stream_summary.bucket_map[2][0].max_error, 1)

            if i == 100:
                stream_summary = c._ss_cache.get_stream_summary()
                c.print_caches()
                self.assertEquals(stream_summary.id_to_bucket_map[2], 6)
                self.assertEquals(stream_summary.bucket_map[6][1].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[7], 6)
                self.assertEquals(stream_summary.bucket_map[6][0].max_error, 5)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 4)
                self.assertEquals(stream_summary.bucket_map[4][1].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[1], 4)
                self.assertEquals(stream_summary.bucket_map[4][0].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 3)
                self.assertEquals(stream_summary.bucket_map[3][0].max_error, 2)

        self.assertEquals([cache_hits, contents], [32, 100])

    def test_dscasw_ibm(self):
        import csv
        c = DataStreamCachingAlgorithmWithSlidingWindowCache(100, monitored=500, subwindow_size=1500, subwindows=2)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/requests_full_ibm.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[0])

                if (contents - 1) % 1500 == 0 or contents % 1500 == 0:
                    print 'number of requests so far: ', contents, 'next:', content
                    c.print_caches()
                    print ''

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertEquals([contents, cache_hits], [60000, 38839])

    def test_dscasw_fastly(self):
        import csv
        c = DataStreamCachingAlgorithmWithSlidingWindowCache(100, monitored=500, subwindow_size=1500, subwindows=10)
        cache_hits = 0
        contents = 0

        with open('../../../resources/Fastly_traces/requests_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

                if contents % 100000 == 0:
                    print contents, 14885146, float(contents) / float(14885146)

        self.assertEquals([contents, cache_hits], [14885146, 1670687])

    def test_adscastk_ibm(self):
        import csv
        c = AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache(500, monitored=1000, window_size=1000)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/requests_full_ibm.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[0])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertEquals([contents, cache_hits], [60000, 39606])
