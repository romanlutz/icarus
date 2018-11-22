
import sys
if sys.version_info[:2] >= (2, 7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The unittest2 package is needed to run the tests.")
del sys

from icarus.models.adaptive_replacement_cache import AdaptiveReplacementCache

class TestAdaptiveReplacementCache(unittest.TestCase):

    def simple_test_arc(self):
        c = AdaptiveReplacementCache(4)
        self.assertEqual(c.get_p(), 0)
        c.put(0)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 1)
        c.put(2)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 2)
        c.put(3)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 3)
        c.put(4)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 4)
        self.assertEqual(c.dump(), [4, 3, 2, 0])
        # last element in top of recency cache is put into bottom
        self.assertEqual(c.put(5), 0)
        self.assertEqual(c.get_p(), 0)
        # 5 moves to frequency cache
        self.assertEqual(c.put(5), None)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 4)
        self.assertEqual(c.dump(), [5, 4, 3, 2])
        # put 2 into frequency cache
        c.get(2)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(c.dump(), [2, 5, 4, 3])
        # put 4 into frequency cache
        c.get(4)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(c.dump(), [4, 2, 5, 3])
        c.clear()
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(len(c), 0)
        self.assertEqual(c.dump(), [])

    def longer_test_arc(self):
        c = AdaptiveReplacementCache(4)
        c.put(1)
        # 1 in frequency cache
        c.put(1)
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(c.dump(), [1])
        c.put(2)
        c.put(3)
        c.put(4)
        # cache full
        self.assertEqual(c.dump(), [1, 4, 3, 2])
        self.assertEqual(c.get_p(), 0)
        self.assertEqual(c.put(5), 2)
        self.assertEqual(c.dump(), [1, 5, 4, 3])
        self.assertEqual(c.get_p(), 0)
        # 2, 1 in frequency cache, 3 in bottom of recency cache
        self.assertEqual(c.put(2), 3)
        self.assertEqual(c.dump(), [2, 1, 5, 4])
        self.assertEqual(c.get_p(), 1)
        # 4, 3 in bottom of recency cache which is now full
        self.assertEqual(c.put(6), 4)
        self.assertEqual(c.dump(), [2, 1, 6, 5])
        # 3 is dropped from bottom of recency cache, 5, 4 still there
        self.assertEqual(c.put(7), 5)
        self.assertEqual(c.dump(), [2, 1, 7, 6])
        self.assertEqual(c.get_p(), 1)
        # 5 goes to frequency cache
        self.assertEqual(c.put(5), 1)
        self.assertEqual(c.get_p(), 2)
        # 4 goes to frequency cache, 2 goes to frequency bottom
        self.assertEqual(c.put(4), 2)
        self.assertEqual(c.dump(), [4, 5, 7, 6])
        self.assertEqual(c.get_p(), 3)


    def test_arc_fastly(self):
        import csv
        c = AdaptiveReplacementCache(100)
        cache_hits = 0
        contents = 0

        with open('../../../resources/Fastly_traces/requests_14M-2015-12-1_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

                if contents % 100000 == 0:
                    print(contents, 14885146, float(contents)/float(14885146))

        self.assertEqual([contents, cache_hits], [14885146, 2679672])
