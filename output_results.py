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
        for k in tree[0]:
            if k[0] == ('workload', 'reqs_file'):
                trace = k[1]
            elif k[0] == ('cache_policy', 'window_size'):
                if k[1] == None:
                    window_size = 0
                else:
                    window_size = int(k[1])
            elif k[0] == ('cache_policy', 'name'):
                policy = k[1]

        for k in tree[1]:
            if k[0] == ('CACHE_HIT_RATIO', 'PER_NODE_CACHE_HIT_RATIO', 1):
                if policy not in rates.keys():
                    rates[policy] = {}
                if window_size not in rates[policy].keys():
                    rates[policy][window_size] = {}
                rates[policy][window_size][trace] = k[1]

        del trace, policy, window_size



    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 1
        for line in csv_reader:
            if i not in range(7, 30):
                traces.append(line[0])
            i += 1

    print ", ".join(traces)

    for policy in rates.keys():
        param_values = rates[policy].keys()
        param_values.sort()
        for param_value in param_values:
            for trace in traces:
                print rates[policy][param_value]['resources/' + trace], '\t',
            print ''

def __main__():
    print_cache_hit_rates()
    #print_results_full()

__main__()
