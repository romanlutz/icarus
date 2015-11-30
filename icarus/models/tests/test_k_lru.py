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