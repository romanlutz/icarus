from icarus.models.cache import SegmentedLruCache, LinkedSet, Cache
from icarus.util import inheritdoc
from icarus.registry import register_cache_policy

__all__ = ['KLruCache']

@register_cache_policy('KLRU')
class KLruCache(Cache):
    """k - Least Recently Used (k-LRU) cache eviction policy.

    This policy divides the cache space into a number of segments of equal
    size each operating according to an LRU policy. When a new item is inserted
    to the cache, it is placed on the top entry of the bottom segment. Each
    subsequent hit promotes the item to the top entry of the segment above.
    When an item is evicted from a segment, it is demoted to the top entry of
    the segment immediately below. An item is evicted from the cache when it is
    evicted from the bottom segment. Only a pre-defined number of segments
    actually contains cached objects. The segments below are simply for observation
    of further elements that might enter the cache in the future.

    This policy can be viewed as a sort of combination between an LRU and LFU
    replacement policies as it makes eviction decisions based both frequency
    and recency of item reference. This specific policy is an extension of
    Segmented-LRU which does not observe elements in addition to the ones
    stored in the cache.
    """

    def __init__(self, maxlen, segments=2, cached_segments=1, **kwargs):
        """Constructor

        Parameters
        ----------
        maxlen : int
            The maximum number of items the cache can store
        segments : int
            The number of segments
        cached_segments : int
            The number of segments which are actually cached
        """
        if maxlen <= 0:
            raise ValueError('maxlen must be positive')
        self._segment_len = maxlen // cached_segments
        if not isinstance(segments, int) or segments <= 0:
            raise ValueError('segments must be an integer and 0 < segments')

        self._cached_segments = cached_segments
        if self._cached_segments < 1:
            raise ValueError('At least one segment needs to be cached')
        if self._cached_segments > segments:
            raise ValueError('Number of cached segments can not be larger than total number of segments')

        self._cache = SegmentedLruCache(maxlen=segments * self._segment_len, segments=segments)

    @inheritdoc(Cache)
    def __len__(self):
        return sum(len(self._cache.get_segment_by_id(id)) for id in range(0, self._cached_segments))

    @property
    @inheritdoc(Cache)
    def maxlen(self):
        return self._segment_len * self._cached_segments

    @inheritdoc(Cache)
    def has(self, k):
        for id in range(0, self._cached_segments):
            if k in self._cache.get_segment_by_id(id):
                return True
        return False

    @inheritdoc(Cache)
    def get(self, k, weight):
        if not self.has(k):
            return False
        else:
            self._cache.get(k)
            return True

    def put(self, k, weight):
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
        last_cached_segment = self._cache.get_segment_by_id(self._cached_segments - 1)
        last_cached_element = list(iter(last_cached_segment))

        self._cache.put(k)

        if self.has(last_cached_element):
            return None
        else:
            return last_cached_element

    @inheritdoc(Cache)
    def remove(self, k):
        return self._cache.remove(k)

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
        if not self.has(k):
            raise ValueError('The item %s is not in the cache' % str(k))
        else:
            return self._cache.position(k)

    @inheritdoc(Cache)
    def dump(self, serialized=True):
        dump = list(list(iter(self._cache.get_segment_by_id(id))) for id in range(0, self._cached_segments))
        return sum(dump, []) if serialized else dump

    @inheritdoc(Cache)
    def clear(self):
        self._cache.clear()
