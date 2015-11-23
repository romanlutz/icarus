__author__ = 'romanlutz'

from icarus.models import Cache
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc
from copy import deepcopy

__all__ = ['SpaceSavingCache',
           'StreamSummary']


@register_cache_policy('SS')
class SpaceSavingCache(Cache):
    """Space Saving cache eviction policy as proposed in

    Metwally, Ahmed, Divyakant Agrawal, and Amr El Abbadi.
    "Efficient computation of frequent and top-k elements in data streams."
    Database Theory-ICDT 2005. Springer Berlin Heidelberg, 2005. 398-412.

    Space Saving uses the so-called Stream-Summary data structure to keep
    track of the approximate number of occurrences of m elements. For that,
    it keeps the estimated number of occurrences and the maximum error
    for each element. For each element, the difference between these two
    numbers is a lower bound for the actual number of occurrences. For k
    of the m elements with k<=m the algorithm guarantees to be the top-k.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, monitored=-1, **kwargs):
        self._maxlen = int(maxlen)
        if self._maxlen < 0:
            raise ValueError('maxlen must be non-negative')
        self._monitored = monitored
        if self._monitored == -1:
            self._monitored = 2 * self._maxlen
        if self._monitored < self._maxlen:
            raise ValueError('Number of monitored elements has to be greater or equal the cache size')
        self._cache = StreamSummary(self._maxlen, self._monitored)

    @inheritdoc(Cache)
    def __len__(self):
        return self._cache.size

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        return self._dump_all()[:self._maxlen]

    def _dump_all(self):
        """
        Creates a list of all monitored elements as opposed to only the cached elements.

        :return: list of all monitored elements at their respective position
        """
        # since the position function needs the list to be sorted from head to tail, the buckets need to be reversed
        list = []
        buckets = self._cache.bucket_map.keys()
        buckets.sort(reverse=True)
        for key in buckets:
            bucket_list = deepcopy(self._cache.bucket_map[key])
            bucket_list.reverse()
            list.extend([node.id for node in bucket_list])
        return list

    def print_buckets(self):
        buckets = self._cache.bucket_map.keys()
        buckets.sort(reverse=True)
        for key in buckets:
            list = deepcopy(self._cache.bucket_map[key])
            list.reverse()
            print key, ':', [(node.id, node.max_error) for node in list]

    def position(self, k):
        """Return the current overall position of an item in the cache. Position *0*
        refers to the head of cache (i.e. most recently used item), while
        position *maxlen - 1* refers to the tail of the cache (i.e. the least
        recently used item).

        This method does not change the internal state of the cache.

        Parameters
        ----------
        k : any hashable type
            The item looked up in the cache

        Returns
        -------
        position : int
            The current position of the item in the cache
        """
        if not k in self.dump():
            raise ValueError('The item %s is not in the cache' % str(k))
        return self._cache.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self.dump()

    @inheritdoc(Cache)
    def get(self, k):
        # search content over the list
        # if it has it push on top, otherwise return false
        if not (self.has(k)):
            return False
        self._cache.add_occurrence(k)
        return True

    def put(self, k):
        """Insert an item in the cache if not already inserted.

        If the element is already present in the cache, it's occurrence counter will be increased.

        Parameters
        ----------
        k : any hashable type
            The item to be inserted

        Returns
        -------
        evicted : any hashable type
            The evicted object or *None* if no contents were evicted.
        """
        return self._cache.add_occurrence(k)

    @inheritdoc(Cache)
    def remove(self, k):
        if not (self.has(k)):
            return False
        return not (self._cache.remove(k) == False)

    @inheritdoc(Cache)
    def clear(self):
        self._cache = StreamSummary(self._maxlen, self._monitored)

    def guaranteed_top_k(self):
        """
        :return: the (maximum) number k for which the underlying StreamSummary data structure guarantees to contain
        the top k elements
        """
        return self._cache.guaranteed_top_k()


class StreamSummary:
    """The StreamSummary data structure was proposed in

    Metwally, Ahmed, Divyakant Agrawal, and Amr El Abbadi.
    "Efficient computation of frequent and top-k elements in data streams."
    Database Theory-ICDT 2005. Springer Berlin Heidelberg, 2005. 398-412.

    It essentially keeps track of the number of occurrences of a number of data objects in a stream. The number of
    observable objects is limited, though, which is why StreamSummary keeps track of a limited number of objects and
    both the estimated number of occurrences and the maximum error. Each object is represented as a node in the data
    structure. The node contains the id of the object and the maximum error. The node is always inserted into a bucket
    that has an index representing the estimated number of occurrences. All these buckets together make up the
    StreamSummary data structure.
    """

    class Node:
        """Nodes are inserted into the StreamSummary data structure. The estimated occurrence counter does not need
        to be set since the node will be inserted into the bucket representing the corresponding estimated occurrence
        counter. Instead, the node contains an upper bound on the maximum error that is included in the estimated
        occurrence counter and the ID representing the cached content itself.
        """

        def __init__(self, id, max_error):
            self.id = id
            self.max_error = max_error

    def __init__(self, size, monitored_items = -1):
        self.id_to_bucket_map = {}
        self.bucket_map = {}
        self.max_size = size  # max cache size
        self.size = 0  # number of currently monitored items
        self.monitored_items = monitored_items
        self.last_cached_bucket = 1  # bucket of last cached item
        self.last_cached_index = 0  # index of last cached item in bucket's list

    def add_occurrence(self, id):
        # node already exists
        if id in self.id_to_bucket_map.keys():
            bucket = self.id_to_bucket_map[id]
            node, index = self.index(bucket=bucket, id=id)
            del self.bucket_map[bucket][index]
            if len(self.bucket_map[bucket]) == 0:
                del self.bucket_map[bucket]

            new_bucket = bucket + 1
            self.insert_node_into_bucket(node, new_bucket)

            if self.size > self.max_size:
                # potentially changes to last_cached pointers
                if new_bucket == self.last_cached_bucket:
                    # two cases:
                    # 1. insertion before last cached item -> shift index by 1
                    # 2. insertion after last cached item -> shift index by 1
                    self.last_cached_index += 1
                elif new_bucket == self.last_cached_bucket + 1:
                    if len(self.bucket_map[self.last_cached_bucket]) == self.last_cached_index + 1:
                        # move last_cached pointers to new_bucket
                        self.last_cached_bucket = new_bucket
                        self.last_cached_index = 0

            return None

        # new node has to be created for id
        else:
            # check if a node has to be dropped
            if self.size == self.max_size:
                min_bucket = min(self.bucket_map.keys())
                del_id = self.bucket_map[min_bucket][0].id

                del self.id_to_bucket_map[del_id]
                evicted_node = self.bucket_map[min_bucket][0]
                del self.bucket_map[min_bucket][0]
                if len(self.bucket_map[min_bucket]) == 0:
                    del self.bucket_map[min_bucket]

                # insert new node at min_bucket+1
                # with error of min_bucket
                node = self.Node(id=id, max_error=min_bucket)
                self.insert_node_into_bucket(node, min_bucket + 1)

                return evicted_node.id

            # no node has to be dropped, cache not full yet
            else:
                self.size += 1
                node = self.Node(id=id, max_error=0)
                self.insert_node_into_bucket(node=node, bucket=1)
                return None

    def insert_node_into_bucket(self, node, bucket):
        self.id_to_bucket_map[node.id] = bucket
        # if bucket exists, insert node at corresponding index
        if bucket in self.bucket_map.keys():
            self.insert_at_correct_index(bucket, node)
        # if bucket doesn't exist, create bucket
        else:
            self.bucket_map[bucket] = [node]

    def insert_at_correct_index(self, bucket, node):
        index = -1
        list = self.bucket_map[bucket]
        for i in range(len(list)):
            if node.max_error > list[i].max_error:
                index = i
                break
        if index == -1:
            self.bucket_map[bucket].append(node)
        else:
            self.bucket_map[bucket].insert(index, node)

    def index(self, bucket, id):
        for index in range(len(self.bucket_map[bucket])):
            if self.bucket_map[bucket][index].id == id:
                return self.bucket_map[bucket][index], index
        return None, None

    def remove(self, id):
        if not (id in self.id_to_bucket_map.keys()):
            return False
        bucket = self.id_to_bucket_map[id]
        del self.id_to_bucket_map[id]
        node, index = self.index(bucket, id)
        if index == None:
            return False
        else:
            del self.bucket_map[bucket][index]

            # the last_cached pointers need to be adjusted
            if self.last_cached_bucket < bucket:
                # one more element can be cached
                if self.last_cached_index > 0:
                    self.last_cached_index -= 1
                else:
                    buckets = self.bucket_map.keys()
                    buckets.sort()
                    bucket_index = buckets.index(bucket)
                    if bucket_index > 0:
                        self.last_cached_bucket = buckets[bucket_index - 1]
                        self.last_cached_index = len(self.bucket_map[self.last_cached_bucket]) - 1
                    else:
                        # there is no bucket below, all objects are removed
                        self.last_cached_bucket = 1

            elif self.last_cached_bucket == bucket:
                if self.last_cached_index == 0:
                    buckets = self.bucket_map.keys()
                    buckets.sort()
                    bucket_index = buckets.index(bucket)
                    if bucket_index > 0:
                        self.last_cached_bucket = buckets[bucket_index - 1]
                        self.last_cached_index = len(self.bucket_map[self.last_cached_bucket]) - 1
                    else:
                        # there is no bucket below, all objects are removed
                        self.last_cached_bucket = 1
                else:
                    self.last_cached_index -= 1

            return node

    def guaranteed_top_k(self):
        buckets = self.bucket_map.keys()
        buckets.sort()
        curr_bucket_index = len(buckets) - 1
        top_bucket_length = len(self.bucket_map[buckets[curr_bucket_index]])
        next_bucket_index = curr_bucket_index if top_bucket_length > 1 else curr_bucket_index - 1
        curr_list_index = top_bucket_length - 1
        curr_guaranteed_occurrences = self.__guaranteed_occurrences(buckets, curr_bucket_index, curr_list_index)
        next_list_index = curr_list_index - 1 if top_bucket_length > 1 else len(
            self.bucket_map[buckets[next_bucket_index]]) - 1
        guaranteed_counter = 0

        for _ in range(self.size - 1):
            next_max_occurrences = buckets[next_bucket_index]
            if curr_guaranteed_occurrences < next_max_occurrences:
                break

            guaranteed_counter += 1

            curr_bucket_index = next_bucket_index
            curr_list_index = next_list_index
            curr_guaranteed_occurrences = self.__guaranteed_occurrences(buckets, curr_bucket_index, curr_list_index)
            if curr_list_index == 0:
                # switch to next bucket
                next_bucket_index = curr_bucket_index - 1
                next_list_index = len(self.bucket_map[buckets[next_bucket_index]]) - 1
            else:
                # move to next element within same bucket
                next_list_index = curr_list_index - 1

        return guaranteed_counter

    def __guaranteed_occurrences(self, buckets, bucket_index, list_index):
        return buckets[bucket_index] - self.bucket_map[buckets[bucket_index]][list_index].max_error
