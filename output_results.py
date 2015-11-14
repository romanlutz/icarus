__author__ = 'romanlutz'
from icarus.results.readwrite import read_results_pickle

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
            if k[0] == ('cache_policy', 'window_size'):
                if k[1] == None:
                    window_size = 0
                else:
                    window_size = int(k[1])
            elif k[0] == ('cache_policy', 'name'):
                policy = k[1]
                if policy not in rates.keys():
                    rates[policy] = {}

        for k in tree[1]:
            if k[0] == ('CACHE_HIT_RATIO', 'PER_NODE_CACHE_HIT_RATIO', 1):
                rates[policy][window_size] = k[1]

    del policy

    for policy in rates.keys():
        param_values = rates[policy].keys()
        param_values.sort()
        for param_value in param_values:
            print rates[policy][param_value], policy, param_value

def __main__():
    print_cache_hit_rates()
    #print_results_full()


__main__()
