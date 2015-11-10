from icarus.models import Cache, LinkedSet
from icarus.registry import register_cache_policy
from icarus.util import inheritdoc
from copy import deepcopy

__all__ = ['AdaptiveReplacementCache']

@register_cache_policy('ARC')
class AdaptiveReplacementCache(Cache):
    """Adaptive Replacement Cache (ARC) eviction policy from

    Megiddo, Nimrod, and Dharmendra S. Modha.
    "ARC: A Self-Tuning, Low Overhead Replacement Cache."
    FAST. Vol. 3. 2003.

    "ARC dynamically, adaptively, and continually balances between the recency and frequency components in an online
    and self-tuning fashion" (from the paper above)
    ARC uses two different lists with observed elements, one of which is based on recently (and only once) observed
    elements and one of which is based on frequently (and at least twice) observed elements. The number of observed
    elements is twice the actual cache size and the percentage of elements in the actual cache from the two lists
    varies based on the stream of requests. In the implementation, the two lists are actually realized as two lists
    each with a top part and a bottom part. The top part corresponds to the part of the list that is in the actual
    cache, while the bottom part consists solely of observed elements that are not in the cache.
    """

    @inheritdoc(Cache)
    def __init__(self, maxlen, **kwargs):
        # ARC has two caches: a recency cache and a frequency cache
        self._recency_cache_top = LinkedSet()  # paper: T_1
        self._recency_cache_bottom = LinkedSet()  # paper: B_1
        self._frequency_cache_top = LinkedSet()  # paper: T_2
        self._frequency_cache_bottom = LinkedSet()  # paper: B_2
        self._maxlen = int(maxlen)  # paper: c
        if self._maxlen <= 0:
            raise ValueError('maxlen must be positive')
        # _p is the target number of elements for the recency cache top
        # which means that _maxlen - _p is the target number of elements for the frequency cache top
        # It is the target number since it will be a real number at some point and not a natural number.
        self._p = 0

    @inheritdoc(Cache)
    def __len__(self):
        return self._frequency_cache_top.__len__() + self._recency_cache_top.__len__()

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._maxlen

    @inheritdoc(Cache)
    def dump(self):
        top_parts = deepcopy(self._frequency_cache_top)
        top_parts.extend(self._recency_cache_top)
        return top_parts

    def position(self, k):
        """Return the current position of an item in the cache. Position *0*
        refers to the head of the frequency cache (i.e. most recently used item
        that was used at least twice), while position *maxlen - 1* refers to the
        tail of the cache (i.e. the least recently used item of the cached items).

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
        cache = self.dump()
        if k not in cache:
            raise ValueError('The item %s is not in the cache' % str(k))
        return cache.index(k)

    @inheritdoc(Cache)
    def has(self, k):
        return k in self.dump()

    @inheritdoc(Cache)
    def get(self, k):
        # two basic cases:
        # 1. cache hit
        # 2. cache miss
        if k not in self.dump():
            # cache miss - do not change state of cache (subsequent put() operation can do that)
            return False
        else:
            # cache hit - change state of cache by moving up k
            if k in self._recency_cache_top:
                # move k to top of frequency cache
                self._recency_cache_top.remove(k)
                self._frequency_cache_top.append_top(k)

            else:  # k is in frequency cache
                self._frequency_cache_top.move_to_top(k)

            return True

    def put(self, k):
        """Register an item's occurrence with the cache if not already inserted.
        This operation changes the state of the cache in that it might influence
        the number of elements taken from the recency and frequency caches and
        possibly replace an element from one of them.

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
        # in case of k already being cached, move it to the top of the frequency cache
        if self.get(k):
            return None
        # there are three cases:
        # 1. k is in bottom of recency cache (i.e. not cached, only observed)
        # 2. k is in bottom of frequency cache (i.e. not cached, only observed)
        # 3. k is in neither of the caches (i.e. neither cached nor observed)
        if k in self._recency_cache_bottom:
            rec = self._recency_cache_bottom.__len__()
            freq = self._frequency_cache_bottom.__len__()
            delta = 1 if rec >= freq else float(freq) / float(rec)
            self._p = min(self._p + delta, self._maxlen)
            evicted = self._replace(k, self._p)
            self._recency_cache_bottom.remove(k)
            self._frequency_cache_top.append_top(k)
            return evicted

        elif k in self._frequency_cache_bottom:
            rec = self._recency_cache_bottom.__len__()
            freq = self._frequency_cache_bottom.__len__()
            delta = 1 if freq >= rec else float(rec) / float(freq)
            self._p = max(self._p - delta, 0)
            evicted = self._replace(k, self._p)
            self._frequency_cache_bottom.remove(k)
            self._frequency_cache_top.append_top(k)
            return evicted

        else:
            # there are two subcases:
            # A) recency cache has _maxlen elements, i.e. it is full
            # B) recency cache is not full yet
            # only in some cases an eviction takes place so the default value is set to None
            evicted = None

            # A) recency cache full
            if self._recency_cache_top.__len__() + self._recency_cache_bottom.__len__() == self._maxlen:
                if self._recency_cache_top < self._maxlen:
                    # the actual eviction is from the top of the recency or frequency cache
                    # dropping from the bottom is not an eviction, it is merely the end of observation
                    self._recency_cache_bottom.pop_bottom()
                    evicted = self._replace(k, self._p)
                else:  # bottom of recency cache is empty
                    evicted = self._recency_cache_top.pop_bottom()

            # B) recency cache not full yet
            else:
                total_observed = self._frequency_cache_top.__len__() + self._frequency_cache_bottom.__len__() + \
                                 self._recency_cache_top.__len__() + self._recency_cache_bottom.__len__()
                if total_observed >= self._maxlen:
                    if total_observed == 2 * self._maxlen:
                        self._frequency_cache_bottom.pop_bottom()
                    evicted = self._replace(k, self._p)
                else:
                    # if the number of observed elements is less than the actual cache size, no removals are necessary
                    pass

            self._recency_cache_top.append_top(k)
            return evicted

    def _replace(self, k, p):
        # REPLACE subroutine from paper - to be used internally only
        rec_top_len = self._recency_cache_top.__len__()
        if rec_top_len > 0 and (rec_top_len > p or (k in self._frequency_cache_bottom and rec_top_len == p)):
            evicted = self._recency_cache_top.pop_bottom()
            self._recency_cache_bottom.append_top(evicted)
            return evicted
        else:
            evicted = self._frequency_cache_top.pop_bottom()
            self._frequency_cache_bottom.append_top(evicted)
            return evicted

    @inheritdoc(Cache)
    def remove(self, k):
        if k in self._frequency_cache_top:
            self._frequency_cache_top.remove(k)
            return True
        elif k in self._frequency_cache_bottom:
            self._frequency_cache_bottom.remove(k)
            return True
        elif k in self._recency_cache_top:
            self._recency_cache_top.remove(k)
            return True
        elif k in self._recency_cache_bottom:
            self._recency_cache_bottom.remove(k)
            return True
        else:
            return False

    @inheritdoc(Cache)
    def clear(self):
        self._frequency_cache_bottom.clear()
        self._frequency_cache_top.clear()
        self._recency_cache_bottom.clear()
        self._recency_cache_top.clear()
        self._p = 0
