"""This package contains implementations of models of cache replacement
policies and caching and routing strategies. 
"""
from .cache import *
from .cachenet import *
from .strategy import *
from .space_saving import *
from .data_stream_caching_algorithm import *
from .adaptive_replacement_cache import *

def policy_parameter_usage(policy):
    # the returned values indicate whether a parameter is used by the queried policy
    if policy == 'LRU':
        return {'window_size': False,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    elif policy == 'KLRU':
        return {'window_size': False,
                'segments': True,
                'cached_segments': True,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    elif policy == 'ARC':
        return {'window_size': False,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    elif policy == 'DSCA':
        return {'window_size': True,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    elif policy == 'DSCASW':
        return {'window_size': False,
                'segments': False,
                'cached_segments': False,
                'subwindows': True,
                'subwindow_size': True,
                'lru_portion': False}
    elif policy == 'DSCAFS':
        return {'window_size': True,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': True}
    elif policy == 'ADSCASTK':
        return {'window_size': True,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    elif policy == 'ADSCAATK':
        return {'window_size': True,
                'segments': False,
                'cached_segments': False,
                'subwindows': False,
                'subwindow_size': False,
                'lru_portion': False}
    else:
        raise ValueError('policy %s unknown' % policy)
