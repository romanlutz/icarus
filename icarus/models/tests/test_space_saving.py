

__author__ = 'romanlutz'


import sys
if sys.version_info[:2] >= (2, 7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        raise ImportError("The unittest2 package is needed to run the tests.")
del sys

from icarus.models.space_saving import SpaceSavingCache, StreamSummary


class TestStreamSummary(unittest.TestCase):

    def test_init(self):
        ss = StreamSummary(5, 5)
        self.assertEqual(ss.id_to_bucket_map, {})
        self.assertEqual(ss.bucket_map, {})
        self.assertEqual(ss.max_size, 5)
        self.assertEqual(ss.size, 0)

    def test_add_a_few(self):
        ss = StreamSummary(5, 5)
        self.assertEqual(ss.size, 0)
        ss.add_occurrence(1)
        self.assertEqual(ss.size, 1)
        ss.add_occurrence(17)
        self.assertEqual(ss.size, 2)
        ss.add_occurrence(23)
        self.assertEqual(ss.size, 3)
        ss.add_occurrence(2)
        self.assertEqual(ss.size, 4)
        ss.add_occurrence(7)
        self.assertEqual(ss.size, 5)
        self.assertSetEqual(set(ss.id_to_bucket_map.keys()), set([1, 17, 23, 2, 7]))
        self.assertSetEqual(set(ss.bucket_map), set([1]))
        self.assertEqual([node.id for node in ss.bucket_map[1]], [1, 17, 23, 2, 7])
        ss.add_occurrence(10)
        self.assertEqual(ss.size, 5)
        self.assertEqual([node.id for node in ss.bucket_map[1]], [17, 23, 2, 7])
        self.assertSetEqual(set(ss.id_to_bucket_map.keys()), set([17, 23, 2, 7, 10]))
        for node in ss.bucket_map[1]:
            self.assertEqual(node.max_error, 0)
        self.assertSetEqual(set(ss.bucket_map), set([1, 2]))
        self.assertEqual(ss.id_to_bucket_map[10], 2)
        self.assertEqual(ss.bucket_map[2][0].max_error, 1)
        ss.add_occurrence(10)
        ss.add_occurrence(10)
        ss.add_occurrence(10)
        ss.add_occurrence(10)
        ss.add_occurrence(10)
        ss.add_occurrence(10)
        self.assertSetEqual(set(ss.bucket_map), set([1, 8]))
        self.assertEqual(ss.bucket_map[8][0].max_error, 1)
        ss.add_occurrence(1) # 2nd occ (but previously evicted)
        self.assertEqual([node.id for node in ss.bucket_map[2]], [1])
        ss.add_occurrence(2) # 2nd occ
        self.assertEqual([node.id for node in ss.bucket_map[2]], [1, 2])
        ss.add_occurrence(3)
        self.assertEqual([node.id for node in ss.bucket_map[2]], [1, 3, 2])
        ss.add_occurrence(4)
        self.assertEqual([node.id for node in ss.bucket_map[2]], [1, 3, 4, 2])
        ss.add_occurrence(5)
        self.assertEqual([node.id for node in ss.bucket_map[2]], [3, 4, 2])
        ss.add_occurrence(10) # 9th occ

        # stream so far: 1, 17, 23, 2, 7, 10x8, 1, 2, 3, 4, 5, 10
        # StreamSummary should have: (id, max_error)
        # bucket 9: (10, 1)
        # bucket 3: (5, 2)
        # bucket 2: (3, 1), (4, 1), (2, 0)
        self.assertSetEqual(set(ss.id_to_bucket_map.keys()), set([2, 3, 4, 5, 10]))
        self.assertSetEqual(set(ss.bucket_map), set([2, 3, 9]))
        self.assertEqual(ss.bucket_map[3][0].max_error, 2)
        self.assertEqual([node.max_error for node in ss.bucket_map[2]], [1, 1, 0])

        top_k = ss.guaranteed_top_k(5)
        self.assertEqual(top_k, [0]) # only 10 is guaranteed

        ss.add_occurrence(5)
        ss.add_occurrence(5)
        self.assertEqual(ss.id_to_bucket_map[5], 5)
        top_k = ss.guaranteed_top_k(5)
        self.assertEqual(top_k, [0]) # 10, 5 and 2 are guaranteed

    def test_youtube(self):
        import csv
        c = SpaceSavingCache(1000, monitored=2000)
        cache_hits = 0
        contents = 0

        print('starting YouTube test')
        with open('../../../resources/UMass_YouTube_traces/YouTube_Trace_7days_reformatted.trace', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                contents += 1
                content = int(row[2])

                if c.get(content):
                    cache_hits += 1
                else:
                    c.put(content)

                if contents % 10000 == 0:
                    print(contents, 258673, float(contents) / float(258673))

        self.assertListEqual([contents, cache_hits], [258673, 36816])


if __name__ == "__main__":
    unittest.main()


