__author__ = 'romanlutz'
from icarus.results.readwrite import read_results_pickle
import csv

def print_results_full():
    result = read_results_pickle('results.pickle')
    for tree in result:
        for k in tree[0]:
            print k
        for k in tree[1]:
            print k

def print_cache_hit_rates():
    result = read_results_pickle('results.pickle')
    rates = {}

    for tree in result:
        window_size, segments, cached_segments, subwindows, subwindow_size, lru_portion = None, None, None, None, None, None
        for k in tree[0]:
            if k[0] == ('workload', 'reqs_file'):
                trace = k[1]

            elif k[0] == ('cache_policy', 'name'):
                policy = k[1]

            elif k[0] == ('cache_policy', 'window_size'):
                if k[1] is not None:
                    window_size = int(k[1])

            elif k[0] == ('cache_policy', 'segments'):
                if k[1] is not None:
                    segments = int(k[1])

            elif k[0] == ('cache_policy', 'cached_segments'):
                if k[1] is not None:
                    cached_segments = int(k[1])

            elif k[0] == ('cache_policy', 'subwindows'):
                if k[1] is not None:
                    subwindows = int(k[1])

            elif k[0] == ('cache_policy', 'subwindow_size'):
                if k[1] is not None:
                    subwindow_size = int(k[1])

            elif k[0] == ('cache_policy', 'lru_portion'):
                if k[1] is not None:
                    lru_portion = float(k[1])

        for k in tree[1]:
            if k[0] == ('CACHE_HIT_RATIO', 'PER_NODE_CACHE_HIT_RATIO', 1):
                if policy not in rates.keys():
                    rates[policy] = {}
                for param in [window_size, segments, subwindow_size]:
                    if param is not None and param not in rates[policy].keys():
                        rates[policy][param] = {}
                if cached_segments is not None and cached_segments not in rates[policy][segments].keys():
                    rates[policy][segments][cached_segments] = {}
                if subwindows is not None and subwindows not in rates[policy][subwindow_size].keys():
                    rates[policy][subwindow_size][subwindows] = {}
                if lru_portion is not None and lru_portion not in rates[policy][window_size].keys():
                    rates[policy][window_size][lru_portion] = {}

                if policy == 'LRU':
                    rates[policy][trace] = k[1]
                elif policy == 'KLRU':
                    rates[policy][segments][cached_segments][trace] = k[1]
                elif policy == 'ARC':
                    rates[policy][trace] = k[1]
                elif policy == 'DSCA':
                    rates[policy][window_size][trace] = k[1]
                elif policy == 'DSCASW':
                    rates[policy][subwindow_size][subwindows][trace] = k[1]
                elif policy == 'DSCAFS':
                    rates[policy][window_size][lru_portion][trace] = k[1]
                elif policy == 'ADSCASTK':
                    rates[policy][window_size][trace] = k[1]
                elif policy == 'ADSCAATK':
                    rates[policy][window_size][trace] = k[1]
                else:
                    print 'error: policy', policy, 'unknown'

    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 1
        for line in csv_reader:
            traces.append(line[0])
            i += 1

    print ", ".join(traces)

    policies = ['ARC', 'LRU', 'KLRU', 'DSCA', 'DSCASW', 'DSCAFS', 'ADSCASTK', 'ADSCAATK']

    dict_list = []

    for policy in policies:
        if policy in rates.keys():
            if policy in ['ARC', 'LRU']:
                dict_list.append((policy, rates[policy]))
            elif policy == 'KLRU':
                segment_values = rates[policy].keys()
                segment_values.sort()
                for segment_value in segment_values:
                    cached_segment_values = rates[policy][segment_value].keys()
                    cached_segment_values.sort()
                    for cached_segment_value in cached_segment_values:
                        dict_list.append(('KLRU (%d,%d)' % (segment_value, cached_segment_value),
                                          rates[policy][segment_value][cached_segment_value]))
            elif policy == 'DSCA':
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    dict_list.append(('DSCA %d' % window_size, rates[policy][window_size]))
            elif policy == 'DSCASW':
                subwindow_sizes = rates[policy].keys()
                subwindow_sizes.sort()
                for subwindow_size in subwindow_sizes:
                    subwindows_values = rates[policy][subwindow_size].keys()
                    subwindows_values.sort()
                    for subwindows in subwindows_values:
                        dict_list.append(('DSCASW (%d %d)' % (subwindow_size, subwindows),
                                          rates[policy][subwindow_size][subwindows]))
            elif policy == 'DSCAFS':
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    lru_portions = rates[policy][window_size].keys()
                    lru_portions.sort()
                    for lru_portion in lru_portions:
                        dict_list.append(('DSCAFS (%d %f)' % (window_size, lru_portion),
                                          rates[policy][window_size][lru_portion]))
            elif policy in ['ADSCASTK', 'ADSCAATK']:
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    dict_list.append(('%s %d' % (policy, window_size), rates[policy][window_size]))


    print dict_list
    for result_dict in dict_list:
        print result_dict[0], '\t',
        for trace in traces:
            try:
                print result_dict[1]['resources/' + trace], '\t',
            except:
                print "fail \t",
        print ''


def __main__():
    print_cache_hit_rates()
    #print_results_full()

__main__()
