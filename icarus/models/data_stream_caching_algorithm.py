__author__ = 'romanlutz'

from icarus.models import Cache, LruCache, SpaceSavingCache, NullCache, StreamSummary
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc
from copy import deepcopy


__all__ = ['DataStreamCachingAlgorithmCache',
           'DataStreamCachingAlgorithmWithSlidingWindowCache']

@register_cache_policy('DSCA')
class DataStreamCachingAlgorithmCache(Cache):
    """Data Stream Caching Algorithm (DSCA) eviction policy as proposed in

    Antonio A. Rocha, Mostafa Dehghan, Theodoros Salonidis, Ting He and Don Towsley
    "DSCA: A Data Stream Caching Algorithm"
    CoNEXT workshop CCDWN'15

    SCA is a mix between LRU and Space Saving. It uses Space Saving's property of guaranteeing a certain number k
    of most occurring elements. These will always be in the cache. Since k is variable, the rest of the cache is
    filled with elements as determined by LRU. Initially k is 0, but after the first window of N the top-k guarantee
    can be determined. The top-k elements from a window i will remain in the cache throughout window i+1. In the
    meanwhile, LRU only considers elements that are not among the top-k anyway to avoid redundancy.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, monitored=-1, window_size=1500, **kwargs):
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
        whole_cache = deepcopy(self._guaranteed_top_k)
        whole_cache.extend(self._lru_cache.dump())
        return whole_cache

    def print_caches(self):
        print 'LRU:', self._lru_cache.dump()
        print 'top-k:', self._guaranteed_top_k
        print 'SS-Cache of current window:'
        self._ss_cache.print_buckets()

    def position(self, k):
        """Return the current overall position of an item in the cache. For DSCA, the position is not important since
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
        The Space Saving Cache is then re-initialized. The rest of the cache is then from the LRU cache.
        The elements in the LRU cache carry over from one period to the next.
        """
        self._window_counter = 0
        whole_dump = self._ss_cache.dump()

        new_guaranteed_indices = self._ss_cache.guaranteed_top_k(min(self._maxlen, len(whole_dump) - 1))
        new_k = new_guaranteed_indices.__len__()
        if new_k > self._maxlen:
            new_k = self._maxlen
        prev_k = len(self._guaranteed_top_k)
        print len(whole_dump), new_guaranteed_indices
        self._guaranteed_top_k = [whole_dump[i] for i in new_guaranteed_indices]
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


@register_cache_policy('DSCASW')
class DataStreamCachingAlgorithmWithSlidingWindowCache(DataStreamCachingAlgorithmCache):
    """Based on DSCA, DSCASW considers multiple subwindows that make up the whole window. Whenever a subwindow is full,
    the oldest subwindow expires and is removed. This process eliminates the purely jumping window from DSCA in favor
    of a sliding (or partially jumping) window.
    """

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def __init__(self, maxlen, monitored=-1, subwindow_size=1500, subwindows=2, **kwargs):
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

        # subwindows and subwindow caches
        self._subwindows = subwindows
        if self._subwindows < 1:
            raise ValueError('Number of subwindows is less than one, but it has to be at least 1.')

        # the cache at 0 is the cache for the previous subwindow, the cache at len-1 is the one that will expire next
        self._window_caches = []
        self._ss_cache = SpaceSavingCache(self._monitored, monitored=self._monitored)
        self._guaranteed_top_k = []  # from previous window

        # to keep track of the windows, there is a counter and the (fixed) size of each window
        self._window_counter = 0
        self._subwindow_size = subwindow_size
        if self._subwindow_size < 0:
            raise ValueError('Size of subwindows needs to be positive.')

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def __len__(self):
        return len(self._lru_cache) + len(self._guaranteed_top_k)

    @property
    @inheritdoc(DataStreamCachingAlgorithmCache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def dump(self):
        whole_cache = deepcopy(self._guaranteed_top_k)
        whole_cache.extend(self._lru_cache.dump())
        return whole_cache

    def print_caches(self):
        print 'LRU:', self._lru_cache.dump()
        print 'top-k:', self._guaranteed_top_k
        print 'SS-Cache of current window:'
        self._window_caches[0].print_buckets()
        print 'Cumulative SS-Cache:'
        self._cumulative_cache.print_buckets()

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def position(self, k):
        dump = self.dump()
        if k not in dump:
            raise ValueError('The item %s is not in the cache' % str(k))
        return dump.index(k)

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def has(self, k):
        return k in self.dump()

    @inheritdoc(DataStreamCachingAlgorithmCache)
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

        if self._window_counter >= self._subwindow_size:
            self._end_of_window_operation()

        return lru_hit or top_k_hit

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def put(self, k):
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
        """ At the end of every subwindow the top k from the space saving cache are put into the _guaranteed_top_k list.
        The Space Saving Cache is not re-initialized but instead the expired window will be subtracted.
        The rest of the actual cache is then from the LRU cache. The elements in the LRU cache carry over from
        one period to the next.
        """
        self._window_counter = 0

        self._remove_last_window()
        whole_dump = self._ss_cache.dump()

        new_guaranteed_indices = self._ss_cache.guaranteed_top_k(min(self._maxlen, len(whole_dump) - 1))
        new_k = new_guaranteed_indices.__len__()
        if new_k > self._maxlen:
            new_k = self._maxlen
        prev_k = len(self._guaranteed_top_k)
        self._guaranteed_top_k = [whole_dump[i] for i in new_guaranteed_indices]
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

    def _remove_last_window(self):
        """ Given the current cumulative Space Saving cache and the Stream Summary data structures from the previous
        subwindows that have not yet expired the last window records are removed and all the subwindow records have to
        be updated by subtracting the information from their cumulative data. This will also reset the current Space
        Saving cache.
        """
        expired_window_table = self._window_caches[-1]
        self._window_caches = self._window_caches[:-1]

        # extract Stream Summary table from the current Space Saving cache.
        self._window_caches.insert(0, self._ss_cache.get_stream_summary().convert_to_dictionary())
        self._ss_cache.clear()

        for i, _ in enumerate(self._window_caches):
            self._expire_window(i, expired_window_table)

        # put updated Stream Summary data structure into current SS cache
        new_stream_summary = StreamSummary(self._monitored, monitored_items=self._monitored)

        for (id, frequency, max_error) in self._window_caches[0]:
            new_stream_summary.insert_node_into_bucket(StreamSummary.Node(id, max_error), frequency)

        self._ss_cache._cache = new_stream_summary

    def _expire_window(self, window_index, expired_window_table):
        new_window_elements = set(self._window_caches[window_index].keys())
        old_window_elements = set(expired_window_table.keys())

        # for every element that is still in the table the frequency and error need to be adjusted directly
        for element in list(new_window_elements & old_window_elements):
            self._window_caches[window_index][element]['frequency'] -= expired_window_table[element]['frequency']
            self._window_caches[window_index][element]['max_error'] -= expired_window_table[element]['max_error']

        # find the minimum frequency of all elements that are not in the window table any more
        frequency_values = []
        for element in list(old_window_elements - new_window_elements):
            frequency_values.append(expired_window_table[element]['frequency'])
        min_frequency = min(frequency_values)

        # subtract the minimum frequency of the expired elements from all frequencies and max errors in the newer window
        for element in list(new_window_elements - old_window_elements):
            self._window_caches[window_index][element]['frequency'] -= min_frequency
            self._window_caches[window_index][element]['max_error'] -= min_frequency


