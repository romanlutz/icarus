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
        window_size, segments, cached_segments = None, None, None
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


        for k in tree[1]:
            if k[0] == ('CACHE_HIT_RATIO', 'PER_NODE_CACHE_HIT_RATIO', 1):
                if policy not in rates.keys():
                    rates[policy] = {}
                for param in [window_size, segments]:
                    if param is not None and param not in rates[policy].keys():
                        rates[policy][param] = {}
                if cached_segments is not None and cached_segments not in rates[policy][segments].keys():
                    rates[policy][segments][cached_segments] = {}

                if policy == 'LRU':
                    rates[policy][trace] = k[1]
                elif policy == 'KLRU':
                    rates[policy][segments][cached_segments][trace] = k[1]
                elif policy == 'ARC':
                    rates[policy][trace] = k[1]
                elif policy == 'DSCA':
                    rates[policy][window_size][trace] = k[1]
                else:
                    print 'error: policy', policy, 'unknown'

    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 1
        for line in csv_reader:
            if i not in [8, 21, 25, 30]:
                traces.append(line[0])
            i += 1

    print ", ".join(traces)

    policies = ['ARC', 'DSCA', 'LRU', 'KLRU']
    dict_list = []

    for policy in policies:
        if policy in ['ARC', 'LRU']:
            dict_list.append((policy, rates[policy]))
        elif policy == 'DSCA':
            window_sizes = rates[policy].keys()
            window_sizes.sort()
            for window_size in window_sizes:
                dict_list.append(('DSCA %d' % window_size, rates[policy][window_size]))
        elif policy == 'KLRU':
            segment_values = rates[policy].keys()
            segment_values.sort()
            for segment_value in segment_values:
                cached_segment_values = rates[policy][segment_value].keys()
                cached_segment_values.sort()
                for cached_segment_value in cached_segment_values:
                    dict_list.append(('KLRU (%d,%d)' % (segment_value, cached_segment_value),
                                      rates[policy][segment_value][cached_segment_value]))

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
