__author__ = 'romanlutz'

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
    def __init__(self, maxlen, **kwargs):
        self._maxlen = int(maxlen)
        if self._maxlen <= 0:
            raise ValueError('maxlen must be positive')
        self._cache = StreamSummary(self._maxlen)


    @inheritdoc(Cache)
    def __len__(self):
        return self._cache.size

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        # since the position function needs the list to be sorted from head to tail, the buckets need to be reversed
        list = []
        for key in sorted(self._cache._bucket_map.keys(), reverse=True):
            list.extend(self._cache._bucket_map[key].reverse())
        return list

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
        return self._cache._id_to_bucket_map.has_key(k)

    @inheritdoc(Cache)
    def get(self, k):
        # search content over the list
        # if it has it push on top, otherwise return false
        if not(self.has(k)):
            return False
        self._cache.add_occurrence(k)
        return True

    def put(self, k):
        """Insert an item in the cache if not already inserted.

        If the element is already present in the cache, it will pushed to the
        top of the cache.

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
        if not(self.has(k)):
            return False
        return not(self._cache.remove(k) == False)

    @inheritdoc(Cache)
    def clear(self):
        self._cache = StreamSummary(self._maxlen)


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

    def __init__(self, size):
        self._id_to_bucket_map = {}
        self._bucket_map = {}
        self.max_size = size
        self.size = 0

    def add_occurrence(self, id):
        # node already exists
        if self._id_to_bucket_map.has_key(id):
            bucket = self._id_to_bucket_map[id]
            node, index = self.index(bucket, id)
            del self._bucket_map[bucket][index]

            new_bucket = bucket + 1
            self.insert_node_into_bucket(node, new_bucket)
            return None

        # new node has to be created for id
        else:
            # check if a node has to be dropped
            if self.size == self.max_size:
                min_bucket = min(self._bucket_map.keys())
                del_id = self._bucket_map[min_bucket][0].id
                del self._id_to_bucket_map[del_id]
                evicted_node = self._bucket_map[min_bucket][0]
                del self._bucket_map[min_bucket][0]

                # insert new node at min_bucket+1
                # with error of min_bucket
                node = Node(id=id, max_error=min_bucket)
                self.insert_node_into_bucket(node, min_bucket+1)

                return evicted_node.id

            # no node has to be dropped, cache not full yet
            else:
                node = Node(id=id, max_error=0)
                self.insert_node_into_bucket(node=node, bucket=1)
                return None

    def insert_node_into_bucket(self, node, bucket):
        self._id_to_bucket_map[id] = bucket
        # if bucket exists, insert node at corresponding index
        if self._bucket_map.has_key(bucket):
            self.insertAtCorrectIndex(node)
        # if bucket doesn't exist, create bucket
        else:
            self._bucket_map[bucket] = [node]

    def insertAtCorrectIndex(self, bucket, node):
        index = -1
        list = self._bucket_map[bucket]
        for i in range(len(list)):
            if node.max_error < list[i].max_error:
                index = i
                break
        if index == -1:
            self._bucket_map[bucket].append(node)
        else:
            self._bucket_map[bucket].insert(index, node)


    def index(self, bucket, id):
        for index in range(len(self._bucket_map[bucket])):
            if self._bucket_map[bucket][index].id == id:
                return list[index], index
        return None, None

    def remove(self, id):
        bucket = self._id_to_bucket_map[id]
        del self._id_to_bucket_map[id]
        node, index = self.index(bucket, id)
        if index == None:
            return False
        else:
            del self._bucket_map[bucket][index]
            return node




