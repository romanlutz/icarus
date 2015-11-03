__author__ = 'romanlutz'

from icarus.models import Cache, LruCache, SpaceSavingCache, NullCache
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc


@register_cache_policy('DSCA')
class DataStreamCachingAlgorithmCache(Cache):
    """Stream Caching Algorithm (SCA) eviction policy as proposed in

    Antonio A. Rocha, Mostafa Dehghan, Theodoros Salonidis, Ting He and Don Towsley
    "SCA: A Data Stream Caching Algorithm"
    CoNEXT workshop CCDWN'15

    SCA is a mix between LRU and Space Saving. It uses Space Saving's property of guaranteeing a certain number k
    of most occurring elements. These will always be in the cache. Since k is variable, the rest of the cache is
    filled with elements as determined by LRU. Initially k is 0, but after the first window of N the top-k guarantee
    can be determined. The top-k elements from a window i will remain in the cache throughout window i+1. In the
    meanwhile, LRU only considers elements that are not among the top-k anyway to avoid redundancy.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, monitored=-1, window_size=-1, **kwargs):
        self._maxlen = int(maxlen)
        if self._maxlen <= 0:
            raise ValueError('maxlen must be positive')
        self._monitored = monitored
        if self._monitored == -1:
            self._monitored = 2 * self._maxlen
        if self._monitored < self._maxlen:
            raise ValueError('Number of monitored elements has to be greater or equal the cache size')
        # initially only LRU
        self._lru_cache = LruCache(self._maxlen)
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        self._guaranteed_top_k = [] # from previous window
        # to keep track of the windows, there is a counter and the (fixed) size of each window
        self._window_size = window_size
        if self._window_size == -1:
            self._window_size = 1000
        if self._window_size <= 0:
            raise ValueError('window_size must be positive')
        self._window_counter = 0

    @inheritdoc(Cache)
    def __len__(self):
        return len(self._lru_cache) + len(self._guaranteed_top_k)

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        return self._guaranteed_top_k.extend(self._lru_cache.dump())

    def print_caches(self):
        print 'LRU:', self._lru_cache.dump()
        print 'top-k:', self._guaranteed_top_k
        print 'SS-Cache of current window:'
        self._ss_cache.print_buckets()

    def position(self, k):
        """Return the current overall position of an item in the cache. For SCA, the position is not important since
        the cache actually consists of two different data structures. The position within each of the data structures
        is important, but this is not returned.

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
        dump = self.dump()
        if k not in dump:
            raise ValueError('The item %s is not in the cache' % str(k))
        return dump.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self.dump()

    @inheritdoc(Cache)
    def get(self, k):
        # check in both LRU and top-k list
        top_k_hit = k in self._guaranteed_top_k
        lru_hit = False
        if not top_k_hit:
            lru_hit = self._lru_cache.get(k)
        # report occurrence to Space Saving
        if lru_hit or top_k_hit:
            self._ss_cache.put(k)
            self._window_counter += 1

        if self._window_counter >= self._window_size:
            self._end_of_window_operation()

        return lru_hit or top_k_hit

    def put(self, k):
        """Insert an item in the cache if not already inserted.

        If the element is already present in the cache, it's occurrence counter will be increased. Also, it will be
        included in the LRU cache if it is not already among the top-k objects.


        Parameters
        ----------
        k : any hashable type
            The item to be inserted

        Returns
        -------
        evicted : any hashable type
            The evicted object or *None* if no contents were evicted.
        """
        if not(k in self._guaranteed_top_k):
            lru_hit = self._lru_cache.get(k)
            if not lru_hit:
                self._lru_cache.put(k)
        self._ss_cache.put(k)

        self._window_counter += 1

        if self._window_counter >= self._window_size:
            self._end_of_window_operation()

        if k in self._guaranteed_top_k:
            return None
        else:
            return self._lru_cache.put(k)

    @inheritdoc(Cache)
    def remove(self, k):
        if k in self._guaranteed_top_k:
            self._guaranteed_top_k.remove(k)
            return True
        else:
            return self._lru_cache.remove(k)

    @inheritdoc(Cache)
    def clear(self):
        self._lru_cache.clear()
        self._guaranteed_top_k = []

    def _end_of_window_operation(self):
        """ At the end of every window the top k from the space saving cache are put into the _guaranteed_top_k list.
        The Space Saving Cache is then re-initialized with the new k which is dependent on the number of guaranteed
        top elements. The rest of the cache is then from the LRU cache. The elements in the LRU cache carry over from
        one period to the next.
        """
        self._window_counter = 0
        new_k = self._ss_cache.guaranteed_top_k()
        if new_k > self._maxlen:
            new_k = self._maxlen
        prev_k = len(self._guaranteed_top_k)
        self._guaranteed_top_k = self._ss_cache.dump()[:new_k]
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        lru_cache_size = self._maxlen - new_k

        for element in self._guaranteed_top_k:
            self._lru_cache.remove(element)

        if new_k == prev_k:
            pass  # continue with current LRU cache
        else:
            lru_elements = self._lru_cache.dump()[:lru_cache_size]
            lru_elements.reverse()
            if new_k == self._maxlen:
                self._lru_cache = NullCache()  # empty LRU cache
            else:
                self._lru_cache = LruCache(lru_cache_size)
                for element in lru_elements:
                    self._lru_cache.put(element)