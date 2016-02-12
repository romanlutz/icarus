__author__ = 'romanlutz'

from icarus.models import Cache, LruCache, SpaceSavingCache, NullCache, StreamSummary, LinkedSet
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc
from copy import deepcopy
import pprint
import numpy as np
pp = pprint.PrettyPrinter(indent=4)



__all__ = ['DataStreamCachingAlgorithmCache',
           'DataStreamCachingAlgorithmWithSlidingWindowCache',
           'DataStreamCachingAlgorithmWithFixedSplitsCache',
           'AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache',
           'AdaptiveDataStreamCachingAlgorithmWithAdaptiveTopKCache',
           'DataStreamCachingAlgorithmWithAdaptiveWindowSizeCache']

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
    def __init__(self, maxlen, monitored=-1, window_size=2000, **kwargs):
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
        self._ss_cache.put(k)
        if not(k in self._guaranteed_top_k):
            if not self._lru_cache.get(k):
                evicted = self._lru_cache.put(k)
                # counter is only increased if there is no cache hit
                # because put should only be called when there is a cache miss
                self._window_counter += 1

                if self._window_counter >= self._window_size:
                    self._end_of_window_operation()

                return evicted
            else:
                # element is in LRU, cache hit
                return None
        else:
            # element is in top-k, cache hit
            return None

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
        prev_top_k = self._guaranteed_top_k
        self._guaranteed_top_k = [whole_dump[i] for i in new_guaranteed_indices]
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        lru_cache_size = self._maxlen - new_k

        for still_existing_element in set(self._guaranteed_top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)

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

        # if there's still space keep some of the otherwise evicted former top-k elements
        if type(self._lru_cache) is not NullCache:
            while len(self._lru_cache) < self._lru_cache._maxlen and len(prev_top_k) > 0:
                self._lru_cache._cache.append_bottom(prev_top_k[0])
                prev_top_k = prev_top_k[1:]


@register_cache_policy('DSCASW')
class DataStreamCachingAlgorithmWithSlidingWindowCache(DataStreamCachingAlgorithmCache):
    """Based on DSCA, DSCASW considers multiple subwindows that make up the whole window. Whenever a subwindow is full,
    the oldest subwindow expires and is removed. This process eliminates the purely jumping window from DSCA in favor
    of a sliding (or partially jumping) window.
    """

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def __init__(self, maxlen, monitored=-1, subwindow_size=2000, subwindows=2, **kwargs):
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
        if self._subwindows < 2:
            raise ValueError('Number of subwindows is less than 2, but it has to be at least 2.')

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

        print 'Cumulative SS-Cache:'
        self._ss_cache.print_buckets()

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
        self._ss_cache.put(k)
        if not(k in self._guaranteed_top_k):
            if not self._lru_cache.get(k):
                evicted = self._lru_cache.put(k)
                # counter is only increased if there is no cache hit
                # because put should only be called when there is a cache miss
                self._window_counter += 1

                if self._window_counter >= self._subwindow_size:
                    self._end_of_window_operation()

                return evicted
            else:
                # element is in LRU, cache hit
                return None
        else:
            # element is in top-k, cache hit
            return None

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

        self._window_caches.insert(0, self._ss_cache.get_stream_summary().convert_to_dictionary())
        if len(self._window_caches) >= self._subwindows:
            self._remove_last_window()

        whole_dump = self._ss_cache.dump()

        new_guaranteed_indices = self._ss_cache.guaranteed_top_k(min(self._maxlen, len(whole_dump) - 1))
        new_k = new_guaranteed_indices.__len__()
        if new_k > self._maxlen:
            new_k = self._maxlen
        prev_k = len(self._guaranteed_top_k)
        prev_top_k = self._guaranteed_top_k
        self._guaranteed_top_k = [whole_dump[i] for i in new_guaranteed_indices]
        lru_cache_size = self._maxlen - new_k

        for still_existing_element in set(self._guaranteed_top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)

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

        # if there's still space keep some of the otherwise evicted former top-k elements
        if type(self._lru_cache) is not NullCache:
            while len(self._lru_cache) < self._lru_cache._maxlen and len(prev_top_k) > 0:
                self._lru_cache._cache.append_bottom(prev_top_k[0])
                prev_top_k = prev_top_k[1:]

    def _remove_last_window(self):
        """ Given the current cumulative Space Saving cache and the Stream Summary data structures from the previous
        subwindows that have not yet expired the last window records are removed and all the subwindow records have to
        be updated by subtracting the information from their cumulative data. This will also reset the current Space
        Saving cache.
        """

        expired_window_table = self._window_caches[-1]
        self._window_caches = self._window_caches[:-1]

        self._ss_cache.clear()

        # expire elements that occurred in expired window by reducing their counts
        for expiring_element_id in expired_window_table:
            self._expire_entry(expiring_element_id, expired_window_table)

        # put updated Stream Summary data structure into current SS cache
        new_stream_summary = StreamSummary(self._monitored, monitored_items=self._monitored)

        for id in self._window_caches[0]:
            new_stream_summary.safe_insert_node(
                StreamSummary.Node(id, self._window_caches[0][id]['max_error']), self._window_caches[0][id]['frequency'])

        self._ss_cache._cache = new_stream_summary


    def _expire_entry(self, expiring_element_id, expired_window_table):
        for i in range(len(self._window_caches) - 1, -1):
            if expiring_element_id not in self._window_caches[i]:
                # element was evicted in this subwindow
                break
            elif self._window_caches[i][expiring_element_id]['max_error'] > expired_window_table[expiring_element_id]['max_error']:
                # error has increased, so element was evicted in this subwindow and occurred again
                break
            else:
                # element is still in the table and has not been evicted
                self._window_caches[i][expiring_element_id]['frequency'] -= expired_window_table[expiring_element_id]['frequency']
                self._window_caches[i][expiring_element_id]['max_error'] -= expired_window_table[expiring_element_id]['max_error']



@register_cache_policy('DSCAFS')
class DataStreamCachingAlgorithmWithFixedSplitsCache(Cache):
    """Similar to DSCA DSCAFS combines two caching strategies. The difference is that DSCA keeps adjusting the size of its LRU cache
    based on the number of top-k guaranteed elements from the previous time window. DSCAFS has a fixed split that is determined
    based on input arguments, e.g. 50% of the cache is LRU and 50% is based on top-k. These top-k need not be guaranteed because
    the number of guaranteed top-k elements might be smaller than k.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, lru_portion = 0.5, monitored=-1, window_size=1500, **kwargs):
        self._maxlen = int(maxlen)
        if self._maxlen <= 0:
            raise ValueError('maxlen must be positive')
        self._monitored = monitored
        if self._monitored == -1:
            self._monitored = 2 * self._maxlen
        if self._monitored < self._maxlen:
            raise ValueError('Number of monitored elements has to be greater or equal the cache size')

        if lru_portion < 0 or lru_portion > 1:
            raise ValueError('The portion of the LRU cache is not valid. It needs to be between 0 and 1.')

        self._lru_cache = LruCache(int(lru_portion * self._maxlen))
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        self._top_k = [] # from previous window
        self._k = self.maxlen - self._lru_cache.maxlen

        # to keep track of the windows, there is a counter and the (fixed) size of each window
        self._window_size = window_size
        if self._window_size <= 0:
            raise ValueError('window_size must be positive')
        self._window_counter = 0

    @inheritdoc(Cache)
    def __len__(self):
        return len(self._lru_cache) + len(self._top_k)

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        whole_cache = deepcopy(self._top_k)
        whole_cache.extend(self._lru_cache.dump())
        return whole_cache

    def print_caches(self):
        print 'LRU:', self._lru_cache.dump()
        print 'top-k:', self._top_k
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
        top_k_hit = k in self._top_k
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
        self._ss_cache.put(k)
        if not(k in self._top_k):
            if not self._lru_cache.get(k):
                evicted = self._lru_cache.put(k)
                # counter is only increased if there is no cache hit
                # because put should only be called when there is a cache miss
                self._window_counter += 1

                if self._window_counter >= self._window_size:
                    self._end_of_window_operation()

                return evicted
            else:
                # element is in LRU, cache hit
                return None
        else:
            # element is in top-k, cache hit
            return None

    @inheritdoc(Cache)
    def remove(self, k):
        if k in self._top_k:
            self._top_k.remove(k)
            return True
        else:
            return self._lru_cache.remove(k)

    @inheritdoc(Cache)
    def clear(self):
        self._lru_cache.clear()
        self._top_k = []

    def _end_of_window_operation(self):
        """ At the end of every window the top k from the space saving cache are put into the _top_k list.
        The Space Saving Cache is then re-initialized. The rest of the cache is then from the LRU cache.
        The elements in the LRU cache carry over from one period to the next.
        """
        self._window_counter = 0
        whole_dump = self._ss_cache.dump()
        prev_top_k = self._top_k
        self._top_k = whole_dump[:self._k]

        for still_existing_element in set(self._top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)

        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)

        for element in self._top_k:
            self._lru_cache.remove(element)

        # append former top-k elements in case LRU cache is not full
        lru_size = len(self._lru_cache)
        while lru_size < self._lru_cache._maxlen and len(prev_top_k) > 0:
            self._lru_cache._cache.append_bottom(prev_top_k[0])
            lru_size += 1
            prev_top_k = prev_top_k[1:]

@register_cache_policy('ADSCASTK')
class AdaptiveDataStreamCachingAlgorithmWithStaticTopKCache(Cache):
    """ADSCASTK is similar to DSCA in that it partitions the cache into a top-k part and a LRU part.
    Unlike DSCA, ADSCA adaptively adjusts the number of elements in the partitions similar to ARC.
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

        # Initially there is only the recency cache
        self._recency_cache_top_length = 0
        self._recency_cache_top = LinkedSet()  # ARC paper: T_1
        self._recency_cache_bottom = LinkedSet()  # ARC paper: B_1
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        self._top_k_cached_length = 0
        self._top_k = []  # from previous window

        # to keep track of the windows, there is a counter and the (fixed) size of each window
        self._window_size = window_size
        if self._window_size <= 0:
            raise ValueError('window_size must be positive')
        self._window_counter = 0

    @inheritdoc(Cache)
    def __len__(self):
        return self._recency_cache_top_length + self._top_k_cached_length

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        whole_cache = deepcopy(self._top_k[:self._top_k_cached_length])
        whole_cache.extend(list(iter(self._recency_cache_top)))
        return whole_cache

    def print_caches(self):
        print 'Recency top:', list(iter(self._recency_cache_top))
        print 'Recency bottom:', list(iter(self._recency_cache_bottom))
        print 'top-k cached:', self._top_k[:self._top_k_cached_length]
        print 'top-k not cached:', self._top_k[self._top_k_cached_length:]
        print 'SS-Cache of current window:'
        self._ss_cache.print_buckets()

    def position(self, k):
        """Return the current overall position of an item in the cache. For ADSCA, the position is not important since
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
        return k in self._top_k[:self._top_k_cached_length] or k in self._recency_cache_top

    @inheritdoc(Cache)
    def get(self, k):
        # check in both LRU and top-k list
        top_k_hit = k in self._top_k[:self._top_k_cached_length]
        lru_hit = False
        if not top_k_hit:
            lru_hit = k in self._recency_cache_top
            # update the recency cache by moving k to the MRU spot
            if lru_hit:
                self._recency_cache_top.move_to_top(k)
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
        self._ss_cache.put(k)

        # cache hit in recency cache
        if k in self._recency_cache_top:
            self._recency_cache_top.move_to_top(k)
            return None
        # cache hit in top_k
        elif k in self._top_k[:self._top_k_cached_length]:
            return None
        # cache miss but hit on observed element in recency cache
        elif k in self._recency_cache_bottom:
            evicted = self._top_k[self._top_k_cached_length - 1]
            self._top_k_cached_length -= 1
            self._recency_cache_bottom.remove(k)
            self._recency_cache_top.append_top(k)
            self._recency_cache_top_length += 1
            return evicted
        # cache miss but hit on observed element in top_k
        elif k in self._top_k[self._top_k_cached_length:]:
            self._top_k_cached_length += 1
            if self._recency_cache_top_length > 0:
                self._recency_cache_top_length -= 1
                evicted = self._recency_cache_top.pop_bottom()
                self._recency_cache_bottom.append_top(evicted)
                return evicted
            else:
                return None
        # cache miss in both caches and miss on observed elements
        else:
            self._recency_cache_top.append_top(k)
            if self._top_k_cached_length + self._recency_cache_top_length < self.maxlen:
                self._recency_cache_top_length += 1
                return None
            elif self._recency_cache_top_length + len(self._recency_cache_bottom) < self.maxlen:
                evicted = self._recency_cache_top.pop_bottom()
                self._recency_cache_bottom.append_top(evicted)
                return evicted
            else:
                # special case: if the whole LRU list is cached we do not add the bottom element to the bottom list
                if self._recency_cache_top_length == self.maxlen:
                    return self._recency_cache_top.pop_bottom()
                else:
                    self._recency_cache_bottom.pop_bottom()
                    evicted = self._recency_cache_top.pop_bottom()
                    self._recency_cache_bottom.append_top(evicted)
                    return evicted

    @inheritdoc(Cache)
    def remove(self, k):
        if k in self._top_k:
            self._top_k.remove(k)
            if self._top_k_cached_length == self.maxlen:
                self._top_k_cached_length -= 1
                if len(self._recency_cache_bottom) > 0:
                    new = self._recency_cache_bottom.pop_top()
                    self._recency_cache_top.append_bottom(new)
                    self._recency_cache_top_length += 1
            return True
        elif k in self._recency_cache_top:
            self._recency_cache_top.remove(k)
            if len(self._recency_cache_bottom) > 0:
                new = self._recency_cache_bottom.pop_top()
                self._recency_cache_top.append_bottom(new)
            else:
                self._recency_cache_top_length -= 1
                if self._top_k_cached_length < len(self._top_k):
                    self._top_k_cached_length += 1
            return True
        else:
            return self._recency_cache_bottom.remove(k)


    @inheritdoc(Cache)
    def clear(self):
        self._recency_cache_top.clear()
        self._recency_cache_bottom.clear()
        self._recency_cache_top_length = 0
        self._top_k_cached_length = 0
        self._top_k = []

    def _end_of_window_operation(self):
        """ At the end of every window all the elements from the space saving cache are put into the _top_k list.
        The Space Saving Cache is then re-initialized. For every element in the LRU cache that will then be in the
        _top_k list the portion of _top_k that is cached is increased by 1. The rest of the cache is then from the
        LRU cache. The elements in the LRU cache carry over from one period to the next except for the ones that
        are also in the _top_k list.
        """
        self._window_counter = 0
        whole_dump = self._ss_cache.dump()

        prev_top_k = self._top_k
        self._top_k = whole_dump[:self.maxlen]
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)

        for still_existing_element in set(self._top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)

        # set up whole cache to be LRU cache before removing the top-k elements from the LRU cache
        self._top_k_cached_length = 0
        for _ in self._recency_cache_bottom:
            self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
        self._recency_cache_top_length = self._recency_cache_top.__len__()

        # remove top-k elements from LRU cache and adjust counters
        for element in self._top_k:
            if element in self._recency_cache_top:
                self._recency_cache_top.remove(element)
                self._recency_cache_top_length -= 1
                if len(self._top_k) > self._top_k_cached_length:
                    self._top_k_cached_length += 1

        # if there's still space add top-k elements
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and self._top_k_cached_length < len(self._top_k):
            self._top_k_cached_length += 1

        # if there's still space add LRU elements
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and len(self._recency_cache_bottom) > 0:
            self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
            self._recency_cache_top_length += 1

        # if there's still space add elements that were "evicted" from top-k
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and len(prev_top_k) > 0:
            self._recency_cache_top.append_bottom(prev_top_k[0])
            prev_top_k = prev_top_k[1:]
            self._recency_cache_top_length += 1



@register_cache_policy('ADSCAATK')
class AdaptiveDataStreamCachingAlgorithmWithAdaptiveTopKCache(Cache):
    """ADSCAATK is similar to ADSCASTK in that it adaptively adjusts the number of elements in _top_k and the LRU list.
    Unlike ADSCASTK, ADSCAATK actually caches the observed element when a hit on an uncached element in _top_k occurs.
    Therefore, _top_k effectively is also a LRU list, but with fixed elements.
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

        # Initially there is only the recency cache
        self._recency_cache_top_length = 0
        self._recency_cache_top = LinkedSet()  # ARC paper: T_1
        self._recency_cache_bottom = LinkedSet()  # ARC paper: B_1
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        self._top_k_cached_length = 0
        self._top_k_cached = LinkedSet()
        self._top_k_uncached = LinkedSet()

        # to keep track of the windows, there is a counter and the (fixed) size of each window
        self._window_size = window_size
        if self._window_size <= 0:
            raise ValueError('window_size must be positive')
        self._window_counter = 0

    @inheritdoc(Cache)
    def __len__(self):
        return self._recency_cache_top_length + self._top_k_cached_length

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        whole_cache = deepcopy(list(iter(self._top_k_cached)))
        whole_cache.extend(list(iter(self._recency_cache_top)))
        return whole_cache

    def print_caches(self):
        print 'Recency top:', list(iter(self._recency_cache_top))
        print 'Recency bottom:', list(iter(self._recency_cache_bottom))
        print 'top-k cached:', list(iter(self._top_k_cached))
        print 'top-k not cached:', list(iter(self._top_k_uncached))
        print 'SS-Cache of current window:'
        self._ss_cache.print_buckets()

    def position(self, k):
        """Return the current overall position of an item in the cache. For ADSCA, the position is not important since
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
        return k in self._top_k_cached or k in self._recency_cache_top

    @inheritdoc(Cache)
    def get(self, k):
        # check in both LRU and top-k list
        top_k_hit = k in self._top_k_cached
        lru_hit = False  # possibly doesn't need to be checked
        if top_k_hit:
            self._top_k_cached.move_to_top(k)
        else:
            lru_hit = k in self._recency_cache_top
            # update the recency cache by moving k to the MRU spot
            if lru_hit:
                self._recency_cache_top.move_to_top(k)
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
        self._ss_cache.put(k)

        # cache hit in recency cache
        if k in self._recency_cache_top:
            self._recency_cache_top.move_to_top(k)
            return None
        # cache hit in top_k
        elif k in self._top_k_cached:
            self._top_k_cached.move_to_top(k)
            return None
        # cache miss but hit on observed element in recency cache
        elif k in self._recency_cache_bottom:
            evicted = self._top_k_cached.pop_bottom()
            self._top_k_cached_length -= 1
            self._recency_cache_bottom.remove(k)
            self._recency_cache_top.append_top(k)
            self._recency_cache_top_length += 1
            return evicted
        # cache miss but hit on observed element in top_k
        elif k in self._top_k_uncached:
            self._top_k_uncached.remove(k)
            self._top_k_cached.append_top(k)
            self._top_k_cached_length += 1
            if self._recency_cache_top_length > 0:
                self._recency_cache_top_length -= 1
                evicted = self._recency_cache_top.pop_bottom()
                self._recency_cache_bottom.append_top(evicted)
                return evicted
            else:
                return None
        # cache miss in both caches and miss on observed elements
        else:
            self._recency_cache_top.append_top(k)
            if self._top_k_cached_length + self._recency_cache_top_length < self.maxlen:
                self._recency_cache_top_length += 1
                return None
            elif self._recency_cache_top_length + len(self._recency_cache_bottom) < self.maxlen:
                evicted = self._recency_cache_top.pop_bottom()
                self._recency_cache_bottom.append_top(evicted)
                return evicted
            else:
                # special case: if the whole LRU list is cached we do not add the bottom element to the bottom list
                if self._recency_cache_top_length == self.maxlen:
                    return self._recency_cache_top.pop_bottom()
                else:
                    self._recency_cache_bottom.pop_bottom()
                    evicted = self._recency_cache_top.pop_bottom()
                    self._recency_cache_bottom.append_top(evicted)
                    return evicted

    @inheritdoc(Cache)
    def remove(self, k):
        if k in self._top_k_cached:
            self._top_k_cached.remove(k)
            if len(self._top_k_uncached) > 0:
                self._top_k_cached.append_bottom(self._top_k_uncached.pop_top())
            else:
                self._top_k_cached_length -= 1
                if len(self._recency_cache_bottom) > 0:
                    self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
            return True
        elif k in self._top_k_uncached:
            return self._top_k_uncached.remove(k)
        elif k in self._recency_cache_top:
            self._recency_cache_top.remove(k)
            if len(self._recency_cache_bottom) > 0:
                self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
            else:
                self._recency_cache_top_length -= 1
                if len(self._top_k_uncached) > 0:
                    self._top_k_cached.append_bottom(self._top_k_uncached.pop_top())
            return True
        else:
            return self._recency_cache_bottom.remove(k)

    @inheritdoc(Cache)
    def clear(self):
        self._recency_cache_top.clear()
        self._recency_cache_bottom.clear()
        self._recency_cache_top_length = 0
        self._top_k_cached.clear()
        self._top_k_uncached.clear()
        self._top_k_cached_length = 0

    def _end_of_window_operation(self):
        """ At the end of every window all the elements from the space saving cache are put into the _top_k list.
        The Space Saving Cache is then re-initialized. For every element in the LRU cache that will then be in the
        _top_k list the portion of _top_k that is cached is increased by 1. The rest of the cache is then from the
        LRU cache. The elements in the LRU cache carry over from one period to the next except for the ones that
        are also in the _top_k list.
        """
        self._window_counter = 0
        whole_dump = self._ss_cache.dump()

        prev_top_k = list(iter(self._top_k_cached))
        prev_top_k.extend(list(iter(self._top_k_uncached)))
        self._top_k_cached.clear()
        self._top_k_uncached.clear()
        self._top_k_cached_length = 0

        new_top_k = whole_dump[:self.maxlen]
        for still_existing_element in set(new_top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)

        # set up whole cache to be LRU cache before removing the top-k elements from the LRU cache
        for _ in self._recency_cache_bottom:
            self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
        self._recency_cache_top_length = self._recency_cache_top.__len__()

        # remove top-k elements from LRU cache and adjust counters
        for element in new_top_k:
            if element in self._recency_cache_top:
                self._recency_cache_top.remove(element)
                self._recency_cache_top_length -= 1
                self._top_k_cached.append_bottom(element)
                self._top_k_cached_length += 1
            elif element in self._recency_cache_bottom:
                self._recency_cache_bottom.remove(element)
                self._top_k_uncached.append_bottom(element)
            else:
                self._top_k_uncached.append_bottom(element)

        # if there's still space add top-k elements
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and len(self._top_k_uncached) > 0:
            self._top_k_cached.append_bottom(self._top_k_uncached.pop_top())
            self._top_k_cached_length += 1

        # if there's still space add LRU elements
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and len(self._recency_cache_bottom) > 0:
            self._recency_cache_top.append_bottom(self._recency_cache_bottom.pop_top())
            self._recency_cache_top_length += 1

        # if there's still space add elements that were "evicted" from top-k
        while self._recency_cache_top_length + self._top_k_cached_length < self.maxlen and len(prev_top_k) > 0:
            self._recency_cache_top.append_bottom(prev_top_k[0])
            prev_top_k = prev_top_k[1:]
            self._recency_cache_top_length += 1


@register_cache_policy('DSCAAWS')
class DataStreamCachingAlgorithmWithAdaptiveWindowSizeCache(DataStreamCachingAlgorithmCache):
    """Data Stream Caching Algorithm with Adaptive Window Size (DSCAAWS) is similar to DSCA but instead of using a
    pre-defined window size after which the top-k are reset, DSCAAWS uses and adaptive window size. It checks repeatedly
    whether it can accept the hypothesis and otherwise keeps sampling. The length of the period for checking the hypo-
    thesis can be specified. To reduce computational overhead it can be set to a larger value than 1. 1 would
    essentially mean that after every request the hypothesis is checked.
    """

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def __init__(self, maxlen, monitored=-1, hypothesis_check_period=1, hypothesis_check_A=0.33,
                 hypothesis_check_epsilon=0.005, **kwargs):
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
        self._hypothesis_check_period = hypothesis_check_period
        if self._hypothesis_check_period < 1:
            raise ValueError('hypothesis_check_period must be positive')
        self._window_counter = 0
        self._cumulative_window_counter = 0

        # hypothesis check parameter A to control the false positives
        self._hypothesis_check_A = hypothesis_check_A
        if self._hypothesis_check_A <= 0 or self._hypothesis_check_A >= 1:
            raise ValueError('false positive control parameter A is not between 0 and 1')

        # hypothesis check parameter epsilon to specify the tolerance interval
        self._hypothesis_check_epsilon = hypothesis_check_epsilon
        if self._hypothesis_check_epsilon <= 0 or self._hypothesis_check_epsilon >= 1:
            raise ValueError('tolerance parameter epsilon is not between 0 and 1')

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

            if self._window_counter >= self._hypothesis_check_period:
                self._cumulative_window_counter += self._window_counter
                self._window_counter = 0
                # only perform hypothesis check if the number of elements in the SS cache is at least max cache size + 1
                if len(self._ss_cache) > self._maxlen:
                    if self._hypothesis_check():
                        self._end_of_window_operation()
                        self._cumulative_window_counter = 0

        return lru_hit or top_k_hit

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def put(self, k):
        self._ss_cache.put(k)
        if not(k in self._guaranteed_top_k):
            if not self._lru_cache.get(k):
                evicted = self._lru_cache.put(k)
                # counter is only increased if there is no cache hit
                # because put should only be called when there is a cache miss
                self._window_counter += 1

                if self._window_counter >= self._hypothesis_check_period:
                    self._cumulative_window_counter += self._window_counter
                    self._window_counter = 0
                    # only perform hypothesis check if the number of elements in the SS cache is at least max cache size + 1
                    if len(self._ss_cache) > self._maxlen:
                        if self._hypothesis_check():
                            self._end_of_window_operation()
                            self._cumulative_window_counter = 0

                return evicted
            else:
                # element is in LRU, cache hit
                return None
        else:
            # element is in top-k, cache hit
            return None


    def _hypothesis_check(self):
        """The null hypothesis states that the sum of the estimated frequencies of the top-k guaranteed elements is
        within an epsilon of the real frequencies of these elements. This is checked with a function. If the hypothesis
        is not accepted, the algorithm keeps sampling. Rejection is not possible.

        Returns
        -------
        bool: True if the hypothesis is accepted, False otherwise
        """

        _, top_k_frequencies, top_k_guaranteed_frequencies = \
            self._ss_cache.guaranteed_top_k(min(self._maxlen, len(self._ss_cache) - 1), return_frequencies=True)

        def func(x, epsilon, n, m):
            return 0.5 * (((x-epsilon)/x)**m * ((1-x+epsilon)/(1-x))**(n-m) +
                          ((x+epsilon)/x)**m * ((1-x-epsilon)/(1-x))**(n-m))

        top_k_frequencies_percentage = float(top_k_frequencies) / float(self._cumulative_window_counter)
        if self._hypothesis_check_A > func(top_k_frequencies_percentage, self._hypothesis_check_epsilon,
                                           self._cumulative_window_counter, top_k_frequencies) :
            return True
        else:
            return False


    def get_cumulative_window_counter(self):
        return self._cumulative_window_counter




@register_cache_policy('DSCAFT')
class DataStreamCachingAlgorithmWithFrequencyThresholdCache(DataStreamCachingAlgorithmCache):
    """DSCAFT is similar to DSCA. The only difference is that objects are considered for the LFU part only if their
    relative frequency within a window is higher than a given percentage (the threshold).
    """

    @inheritdoc(DataStreamCachingAlgorithmCache)
    def __init__(self, maxlen, monitored=-1, window_size=1500, threshold=0.0025, **kwargs):
        DataStreamCachingAlgorithmCache.__init__(self, maxlen=maxlen, monitored=monitored, window_size=window_size)

        # determine min threshold for LFU consideration
        self.threshold = int(self._window_size * threshold)

    def _end_of_window_operation(self):
        """ At the end of every window the top k from the space saving cache are put into the _guaranteed_top_k list.
        The Space Saving Cache is then re-initialized. The rest of the cache is then from the LRU cache.
        The elements in the LRU cache carry over from one period to the next.
        """
        self._window_counter = 0
        whole_dump = self._ss_cache.dump()

        new_guaranteed_indices = self._ss_cache.guaranteed_top_k(min(self._maxlen, len(whole_dump) - 1))

        # check which elements satisfy the threshold frequency
        for index in new_guaranteed_indices:
            if self._ss_cache._cache.id_to_bucket_map[whole_dump[index]] < self.threshold:
                new_guaranteed_indices.remove(index)

        new_k = len(new_guaranteed_indices)
        if new_k > self._maxlen:
            new_k = self._maxlen
        prev_k = len(self._guaranteed_top_k)
        prev_top_k = self._guaranteed_top_k
        self._guaranteed_top_k = [whole_dump[i] for i in new_guaranteed_indices]
        self._ss_cache = SpaceSavingCache(self._monitored, self._monitored)
        lru_cache_size = self._maxlen - new_k

        for still_existing_element in set(self._guaranteed_top_k) & set(prev_top_k):
            prev_top_k.remove(still_existing_element)

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

        # if there's still space keep some of the otherwise evicted former top-k elements
        if type(self._lru_cache) is not NullCache:
            while len(self._lru_cache) < self._lru_cache._maxlen and len(prev_top_k) > 0:
                self._lru_cache._cache.append_bottom(prev_top_k[0])
                prev_top_k = prev_top_k[1:]

