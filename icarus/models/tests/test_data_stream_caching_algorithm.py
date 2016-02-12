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

from icarus.models.data_stream_caching_algorithm import DataStreamCachingAlgorithmCache, \
    DataStreamCachingAlgorithmWithSlidingWindowCache, AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache, \
    DataStreamCachingAlgorithmWithFrequencyThresholdCache
import pprint

import os

pp = pprint.PrettyPrinter(indent=4)

test_dsca_ibm = False
test_dsca_fastly = False
test_small_sliding_window = False
test_medium_sliding_window = False
test_dscasw_ibm = True
test_dscasw_fastly = True
test_adscastk_ibm = False
test_adscastk_youtube = False
test_dscaft = False

class TestDSCA(unittest.TestCase):
    @unittest.skipUnless(test_dsca_ibm, 'Test DSCA on IBM trace')
    def test_dsca_ibm(self):
        import csv
        c = DataStreamCachingAlgorithmCache(100, monitored=500, window_size=1500)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/anon-url-trace_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if (contents - 1) % 1500 == 0:
                    # assert that end of window operation was executed by checking for SS cache size of 0
                    self.assertEquals(c._ss_cache._cache.size, 0)

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertEquals([contents, cache_hits], [8626163, 2654063])

    @unittest.skipUnless(test_dsca_fastly, 'Test DSCA on Fastly trace')
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

        self.assertEquals(cache_hits, [2205564, 2148198, 2101404, 2054630, 2021315, 1990617, 1933566])

    @unittest.skipUnless(test_small_sliding_window, 'Test DSCASW with a small artificial input stream')
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
                c.print_caches()
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 10)
                self.assertEquals(stream_summary.bucket_map[10][0].max_error, 0)
                self.assertEquals(stream_summary.id_to_bucket_map[8], 9)
                self.assertEquals(stream_summary.bucket_map[9][0].max_error, 3)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 8)
                self.assertEquals(stream_summary.bucket_map[8][1].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[6], 8)
                self.assertEquals(stream_summary.bucket_map[8][0].max_error, 4)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 5)
                self.assertEquals(stream_summary.bucket_map[5][0].max_error, 4)

        self.assertEquals([cache_hits, contents], [25, 40])

    @unittest.skipUnless(test_medium_sliding_window, 'Test DSCASW with a medium sized artificial input stream')
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
            else:
                cache_hits += 1

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
                self.assertEquals(stream_summary.id_to_bucket_map[1], 8)
                self.assertEquals(stream_summary.bucket_map[8][4].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 8)
                self.assertEquals(stream_summary.bucket_map[8][3].max_error, 6)
                self.assertEquals(stream_summary.id_to_bucket_map[2], 8)
                self.assertEquals(stream_summary.bucket_map[8][2].max_error, 7)
                self.assertEquals(stream_summary.id_to_bucket_map[8], 8)
                self.assertEquals(stream_summary.bucket_map[8][1].max_error, 7)
                self.assertEquals(stream_summary.id_to_bucket_map[16], 8)
                self.assertEquals(stream_summary.bucket_map[8][0].max_error, 7)
                self.assertEquals(c._lru_cache.dump(), [2, 8, 16, 3, 7])
                self.assertEquals(c._guaranteed_top_k, [])

            if i == 60:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 12)
                self.assertEquals(stream_summary.bucket_map[12][4].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[7], 12)
                self.assertEquals(stream_summary.bucket_map[12][3].max_error, 11)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 12)
                self.assertEquals(stream_summary.bucket_map[12][2].max_error, 11)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 12)
                self.assertEquals(stream_summary.bucket_map[12][1].max_error, 11)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 12)
                self.assertEquals(stream_summary.bucket_map[12][0].max_error, 11)

            if i == 80:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[1], 17)
                self.assertEquals(stream_summary.bucket_map[17][0].max_error, 2)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 16)
                self.assertEquals(stream_summary.bucket_map[16][2].max_error, 11)
                self.assertEquals(stream_summary.id_to_bucket_map[13], 16)
                self.assertEquals(stream_summary.bucket_map[16][1].max_error, 14)
                self.assertEquals(stream_summary.id_to_bucket_map[3], 16)
                self.assertEquals(stream_summary.bucket_map[16][0].max_error, 14)
                self.assertEquals(stream_summary.id_to_bucket_map[9], 15)
                self.assertEquals(stream_summary.bucket_map[15][0].max_error, 14)

            if i == 100:
                stream_summary = c._ss_cache.get_stream_summary()
                self.assertEquals(stream_summary.id_to_bucket_map[3], 21)
                self.assertEquals(stream_summary.bucket_map[21][0].max_error, 18)
                self.assertEquals(stream_summary.id_to_bucket_map[2], 20)
                self.assertEquals(stream_summary.bucket_map[20][2].max_error, 18)
                self.assertEquals(stream_summary.id_to_bucket_map[1], 20)
                self.assertEquals(stream_summary.bucket_map[20][1].max_error, 18)
                self.assertEquals(stream_summary.id_to_bucket_map[4], 20)
                self.assertEquals(stream_summary.bucket_map[20][0].max_error, 19)
                self.assertEquals(stream_summary.id_to_bucket_map[8], 19)
                self.assertEquals(stream_summary.bucket_map[19][0].max_error, 17)

        self.assertEquals([cache_hits, contents], [31, 100])

    @unittest.skipUnless(test_dscasw_ibm, 'Test DSCASW on IBM trace')
    def test_dscasw_ibm(self):
        import csv
        c = DataStreamCachingAlgorithmWithSlidingWindowCache(1000, monitored=2000, subwindow_size=2000, subwindows=4)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/anon-url-trace_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

                # verify that all elements have a frequency and error that make sense
                if contents % 2000 == 0:
                    ss = c._ss_cache.get_stream_summary().convert_to_dictionary()
                    for element in ss:
                        self.assertGreaterEqual(ss[element]['frequency'], ss[element]['max_error'])
                        self.assertGreaterEqual(ss[element]['max_error'], 0)
                        self.assertGreaterEqual(ss[element]['frequency'], 0)

        self.assertEquals([contents, cache_hits], [8626163, 2983581])

    @unittest.skipUnless(test_dscasw_fastly, 'Test DSCASW on Fastly trace')
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

                # verify that all elements have a frequency and error that make sense
                if contents % 2000 == 0:
                    ss = c._ss_cache.get_stream_summary().convert_to_dictionary()
                    for element in ss:
                        self.assertGreaterEqual(ss[element]['frequency'], ss[element]['max_error'])
                        self.assertGreaterEqual(ss[element]['max_error'], 0)
                        self.assertGreaterEqual(ss[element]['frequency'], 0)

        self.assertEquals([contents, cache_hits], [14885146, 1639996])

    @unittest.skipUnless(test_adscastk_ibm, 'Test ADSCASTK on IBM trace')
    def test_adscastk_ibm(self):
        import csv
        c = AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache(500, monitored=1000, window_size=1000)
        cache_hits = 0
        contents = 0

        with open('../../../resources/IBM_traces/anon-url-trace_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertListEqual([contents, cache_hits], [8626163, 2889917])

    @unittest.skipUnless(test_adscastk_youtube, 'Test ADSCASTK on YouTube trace')
    def test_adscastk_youtube(self):
        import csv
        c = AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache(1000, monitored=2000, window_size=2000)
        cache_hits = 0
        contents = 0

        with open('../../../resources/UMass_YouTube_traces/YouTube_Trace_7days_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertListEqual([contents, cache_hits], [258673, 35758])


    @unittest.skipUnless(test_dscaft, 'Test DSCAFT on YouTube trace')
    def test_dscaft_youtube(self):
        import csv
        c = DataStreamCachingAlgorithmWithFrequencyThresholdCache(1000, monitored=2000, window_size=8000)
        cache_hits = 0
        contents = 0

        with open('../../../resources/UMass_YouTube_traces/YouTube_Trace_7days_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

        self.assertListEqual([contents, cache_hits], [258673, 36039])


if __name__ == "__main__":
    unittest.main()