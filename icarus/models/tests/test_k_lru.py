from __future__ import division
import sys
from icarus.models.k_lru import KLruCache
if sys.version_info[:2] >= (2, 7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The unittest2 package is needed to run the tests.")
del sys


import icarus.models as cache

class TestKLRU(unittest.TestCase):

    def test_k_lru_ibm(self):
        import csv
        c = KLruCache(100, segments=2, cached_segments=1)
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

        self.assertEquals([contents, cache_hits], [60000, 39136])

    def test_klru_fastly(self):
        import csv
        c = [KLruCache(100, segments=2, cached_segments=1),
             KLruCache(100, segments=3, cached_segments=1),
             KLruCache(100, segments=3, cached_segments=2)]
        cache_hits = [0]*3
        contents = 0

        with open('../../../resources/Fastly_traces/requests_14M-2015-12-1_reformatted.trace', 'r') as csv_file:
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

        self.assertEquals(cache_hits, [2677248, 2660332, 2588451])