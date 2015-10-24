__author__ = 'romanlutz'

from icarus.models import Cache, LruCache, SpaceSavingCache
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
            self._window_size = 100
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
        if not k in dump:
            raise ValueError('The item %s is not in the cache' % str(k))
        return dump.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self.dump()

    @inheritdoc(Cache)
    def get(self, k):
        # check in both LRU and top-k list
        lru_hit = self._lru_cache.get(k)
        top_k_hit = k in self._guaranteed_top_k
        # report occurrence to Space Saving
        self._ss_cache.get(k)
        self._window_counter += 1

        if self._window_counter >= self._window_size:


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
        self._ss_cache.put(k)
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
        """ At the end of every window

        """
        self._window_counter = 0
        new_k = self._ss_cache.guaranteed_top_k()
        prev_k = len(self._guaranteed_top_k)
        self._guaranteed_top_k = self._ss_cache.dump()[:new_k]
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        if new_k < prev_k:
            lru_cache_size = self._maxlen - new_k
            lru_elements = self._lru_cache.dump()[:lru_cache_size]
            lru_elements.reverse()
            self._lru_cache = LruCache(lru_cache_size)
            for element in lru_elements:
                self._lru_cache.put(element)


        #TODO: include in put!!!!!!!!
