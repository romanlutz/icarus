__author__ = 'romanlutz'

from icarus.models import Cache
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc
from copy import deepcopy

__all__ = ['SpaceSavingCache',
           'WeightedStreamSummary']


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
    In this implementation SpaceSaving uses WeightedStreamSummary which is
    a generalization of StreamSummary allowing for weighted items.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, monitored=2.0, **kwargs):
        self._maxlen = int(maxlen)
        if self._maxlen < 0:
            raise ValueError('maxlen must be non-negative')
        self._monitored = int(monitored * maxlen)
        if self._monitored < self._maxlen:
            raise ValueError('Number of monitored elements has to be greater or equal the cache size')
        self._cache = WeightedStreamSummary(self._maxlen, self._monitored)

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
        dumped_list = []
        buckets = list(self._cache.bucket_map.keys())
        buckets.sort(reverse=True)
        for key in buckets:
            bucket_list = deepcopy(self._cache.bucket_map[key])
            bucket_list.reverse()
            dumped_list.extend([node.id for node in bucket_list])
        return dumped_list

    def print_buckets(self):
        buckets = list(self._cache.bucket_map.keys())
        buckets.sort(reverse=True)
        for key in buckets:
            list = deepcopy(self._cache.bucket_map[key])
            list.reverse()
            print(("%d: %s" %(key, str([(node.id, node.max_error) for node in list]))))

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
        if not self.has(k):
            raise ValueError('The item %s is not in the cache' % str(k))
        return self._cache.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self._cache.id_to_bucket_map

    @inheritdoc(Cache)
    def get(self, k, weight):
        # search content over the list
        # if it has it push on top, otherwise return false
        if not (self.has(k)):
            return False
        self._cache.add_occurrence(k, weight)
        return True

    def put(self, k, weight):
        """Insert an item in the cache if not already inserted.

        If the element is already present in the cache, it's occurrence counter will be increased.

        Parameters
        ----------
        k : any hashable type
            The item to be inserted
        weight : int
            The weight of the item

        Returns
        -------
        evicted : any hashable type
            The evicted object or *None* if no contents were evicted.
        """
        return self._cache.add_occurrence(k, weight)

    @inheritdoc(Cache)
    def remove(self, k):
        if not (self.has(k)):
            return False
        return not (self._cache.remove(k) == False)

    @inheritdoc(Cache)
    def clear(self):
        self._cache = WeightedStreamSummary(self._maxlen, self._monitored)

    def guaranteed_top_k(self, k, return_frequencies=False):
        """
        :return: the (maximum) number k for which the underlying StreamSummary data structure guarantees to contain
        the top k elements
        """
        return self._cache.guaranteed_top_k(k, return_frequencies)

    def get_stream_summary(self):
        return deepcopy(self._cache)


class WeightedStreamSummary:
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

    WeightedStreamSummary extends this by allowing items to be weighted, that is their occurrences can count more than once.
    """

    class Node:
        """Nodes are inserted into the StreamSummary data structure. The estimated occurrence counter does not need
        to be set since the node will be inserted into the bucket representing the corresponding estimated occurrence
        counter. Instead, the node contains an upper bound on the maximum error that is included in the estimated
        occurrence counter and the ID representing the cached content itself.
        """

        def __init__(self, id, max_error, weight):
            self.id = id
            self.max_error = max_error
            self.weight = weight

    def __init__(self, size, monitored_items):
        self.id_to_bucket_map = {}
        self.bucket_map = {}
        self.max_size = size  # max cache size
        self.size = 0  # number of currently monitored items
        if monitored_items <= 0:
            raise ValueError('monitored items needs to be a positive number.')
        self.monitored_items = monitored_items
        self.last_cached_bucket = 1  # bucket of last cached item
        self.last_cached_index = 0  # index of last cached item in bucket's list

    def add_occurrence(self, id, weight):
        # node already exists
        if id in self.id_to_bucket_map:
            old_bucket = self.id_to_bucket_map[id]
            node, index = self.index(bucket=old_bucket, id=id)
            del self.bucket_map[old_bucket][index]
            if len(self.bucket_map[old_bucket]) == 0:
                del self.bucket_map[old_bucket]

            # one occurrence moves the element up by 'weight' buckets
            new_bucket = old_bucket + weight
            self._insert_node_into_bucket(node, new_bucket)

            if self.size > self.max_size:
                # potentially changes to last_cached pointers
                if new_bucket == self.last_cached_bucket:
                    # two cases:
                    # 1. insertion before last cached item -> shift index by 1
                    # 2. insertion after last cached item -> shift index by 1
                    self.last_cached_index += 1
                elif new_bucket > self.last_cached_bucket:
                    if old_bucket == self.last_cached_bucket:
                        # previously last element index is now beyond the end of the list (= length)
                        if len(self.bucket_map[self.last_cached_bucket]) == self.last_cached_index:
                            # move last_cached pointers to new_bucket
                            self.last_cached_bucket = new_bucket
                            self.last_cached_index = 0
                        else:
                            # there could elements after the former position which would mean we have to advance the
                            # pointer by 1, but since the element itself was removed from the list, all elements after
                            # it have a larger index automatically so nothing needs to be done!
                            pass
                    elif old_bucket < self.last_cached_bucket:
                        # last cached bucket is in between old and new bucket
                        if len(self.bucket_map[self.last_cached_bucket]) == self.last_cached_index + 1:
                            # move last_cached pointers to new_bucket
                            self.last_cached_bucket = new_bucket
                            self.last_cached_index = 0
                        else:
                            # move the last cached index one further within the bucket
                            self.last_cached_index += 1
            return None

        # new node has to be created for id
        else:
            # check if a node has to be dropped
            if self.size == self.max_size:
                min_bucket = min(self.bucket_map.keys())
                evicted_node = self.bucket_map[min_bucket][0]
                evicted_occurrences = min_bucket / evicted_node.weight

                del self.id_to_bucket_map[evicted_node.id]
                del self.bucket_map[min_bucket][0]
                if len(self.bucket_map[min_bucket]) == 0:
                    del self.bucket_map[min_bucket]

                # insert new node at min_bucket+weight with error of (min_bucket/old weight)*new weight
                node = self.Node(id=id, max_error=evicted_occurrences*weight, weight=weight)
                self._insert_node_into_bucket(node, (evicted_occurrences+1) * weight)

                return evicted_node.id

            # no node has to be dropped, cache not full yet
            else:
                self.size += 1
                node = self.Node(id=id, max_error=0, weight=weight)
                self._insert_node_into_bucket(node=node, bucket=weight)
                return None

    def _insert_node_into_bucket(self, node, bucket):
        """ This method is not supposed to be used from outside SpaceSaving. Without appropriate additional calls this
        will compromise the overall data structure!
        insert_node_into_bucket simply inserts the node at the correct position within the bucket and updates the
        bucket_map accordingly.
        """
        self.id_to_bucket_map[node.id] = bucket
        # if bucket exists, insert node at corresponding index
        if bucket in self.bucket_map:
            self._insert_at_correct_index(bucket, node)
        # if bucket doesn't exist, create bucket
        else:
            self.bucket_map[bucket] = [node]

    def safe_insert_node(self, node, bucket):
        """ This method is used to fill a new StreamSummary data structure with existing Node objects. It is necessary
        to update the internal pointers and counters in order to maintain a functionally correct data structure.
        Currently, this method can only insert until the StreamSummary is full. If insertion was successful, it returns
        True, otherwise False.
        """
        if self.size >= self.monitored_items:
            # data structure already full - replacement not implemented so far for safe insert
            return False

        self.size += 1

        self._insert_node_into_bucket(node, bucket)
        # id_to_bucket_map and bucket_map are already up-to-date

        # last_cached_bucket and last_cached_index has to be updated
        if self.size == 1:
            # insertion was first one
            self.last_cached_bucket = bucket
            self.last_cached_index = 0
        elif self.size <= self.max_size:
            # keep bucket pointer at lowest bucket
            if bucket < self.last_cached_bucket:
                self.last_cached_bucket = bucket
        else:
            # we have more elements than we can cache, so the pointers might need to be moved
            if self.last_cached_bucket < bucket:
                # potentially changes to last_cached pointers
                if len(self.bucket_map[self.last_cached_bucket]) == self.last_cached_index + 1:
                    # move last_cached pointers to next bucket
                    self.last_cached_bucket += 1
                    while self.last_cached_bucket not in list(self.bucket_map.keys()):
                        self.last_cached_bucket += 1
                    self.last_cached_index = 0
                else:
                    self.last_cached_index += 1
            elif self.last_cached_bucket == bucket:
                self.last_cached_index += 1

        return True

    def _insert_at_correct_index(self, bucket, node):
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
        if not (id in self.id_to_bucket_map):
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
                    buckets = list(self.bucket_map.keys())
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
                    buckets = list(self.bucket_map.keys())
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

    def guaranteed_top_k(self, k, return_frequencies=False):
        """
        Checks for the elements whose guaranteed frequency is at least as high as the maximum frequency of the k+1th
        element in the data structure.

        Parameters
        ----------
        k - the number of considered elements
        return_frequencies - bool that indicates whether the aggregated frequencies of the guaranteed elements shall
                             be returned or not

        Returns
        -------
        - list of indices where the minimum guaranteed number of occurrences is greater or equal than the maximum
          number of occurrences of the k+1th element.
        - possibly (depending on options) the aggregated frequencies of the guaranteed top k elements shall
          be returned as well

        """
        buckets = list(self.bucket_map.keys())
        buckets.sort()
        # set indices to top element
        curr_bucket_index = len(buckets) - 1
        top_bucket_length = len(self.bucket_map[buckets[curr_bucket_index]])
        next_bucket_index = curr_bucket_index if top_bucket_length > 1 else curr_bucket_index - 1
        curr_list_index = top_bucket_length - 1
        next_list_index = curr_list_index - 1 if top_bucket_length > 1 \
                                              else len(self.bucket_map[buckets[next_bucket_index]]) - 1

        min_guaranteed_frequency = []
        top_k_frequencies = []

        for i in range(k):
            freq, _, guaranteed_occurrences = self.__guaranteed_occurrences(buckets, curr_bucket_index, curr_list_index)
            min_guaranteed_frequency.append(guaranteed_occurrences)
            top_k_frequencies.append(freq)

            curr_bucket_index = next_bucket_index
            curr_list_index = next_list_index
            if curr_list_index == 0:
                # switch to next bucket
                next_bucket_index = curr_bucket_index - 1
                next_list_index = len(self.bucket_map[buckets[next_bucket_index]]) - 1
            else:
                # move to next element within same bucket
                next_list_index = curr_list_index - 1

        max_frequency = buckets[next_bucket_index] if next_bucket_index >= 0 else buckets[0]
        guaranteed_indices = []
        total_top_k_frequency = 0
        total_top_k_occurrences = 0

        for i in range(k):
            if min_guaranteed_frequency[i] >= max_frequency:
                guaranteed_indices.append(i)
                total_top_k_frequency += top_k_frequencies[i]
                total_top_k_occurrences += min_guaranteed_frequency[i]

        if return_frequencies:
            return guaranteed_indices, total_top_k_frequency, total_top_k_occurrences
        else:
            return guaranteed_indices

    def __guaranteed_occurrences(self, buckets, bucket_index, list_index):
        return buckets[bucket_index], self.bucket_map[buckets[bucket_index]][list_index].max_error, \
               buckets[bucket_index] - self.bucket_map[buckets[bucket_index]][list_index].max_error

    def convert_to_dictionary(self):
        # since the position function needs the list to be sorted from head to tail, the buckets need to be reversed
        dict = {}
        buckets = list(self.bucket_map.keys())
        for key in buckets:
            bucket_list = deepcopy(self.bucket_map[key])
            for node in bucket_list:
                dict[node.id] = {'max_error': node.max_error, 'frequency': key, 'weight': node.weight}
        return dict
