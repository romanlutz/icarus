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
    of the m elements with k<=m the algorithm guarantees to be in the top-k.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, **kwargs):
        self._cache = LinkedSet()
        self._maxlen = int(maxlen)
        if self._maxlen <= 0:
            raise ValueError('maxlen must be positive')

    @inheritdoc(Cache)
    def __len__(self):
        return len(self._cache)

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        return list(iter(self._cache))

    def position(self, k):
        """Return the current position of an item in the cache. Position *0*
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
        if not k in self._cache:
            raise ValueError('The item %s is not in the cache' % str(k))
        return self._cache.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self._cache

    @inheritdoc(Cache)
    def get(self, k):
        # search content over the list
        # if it has it push on top, otherwise return false
        if k not in self._cache:
            return False
        self._cache.move_to_top(k)
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
        # if content in cache, push it on top, no eviction
        if k in self._cache:
            self._cache.move_to_top(k)
            return None
        # if content not in cache append it on top
        self._cache.append_top(k)
        return self._cache.pop_bottom() if len(self._cache) > self._maxlen else None

    @inheritdoc(Cache)
    def remove(self, k):
        if k not in self._cache:
            return False
        self._cache.remove(k)
        return True

    @inheritdoc(Cache)
    def clear(self):
        self._cache.clear()


class StreamSummary:

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

        # new node has to be created for id
        else:
            # check if a node has to be dropped
            if self.size == self.max_size:
                min_bucket = min(self._bucket_map.keys())
                del_id = self._bucket_map[min_bucket][0].id
                del self._id_to_bucket_map[del_id]
                del self._bucket_map[min_bucket][0]

                # insert new node at min_bucket+1
                # with error of min_bucket
                node = Node(id=id, max_error=min_bucket)
                self.insert_node_into_bucket(node, min_bucket+1)


            # no node has to be dropped, cache not full yet
            else:
                node = Node(id=id, max_error=0)
                self.insert_node_into_bucket(node=node, bucket=1)

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




