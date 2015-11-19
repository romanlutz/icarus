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



    traces = ['IBM_traces/requests_full_ibm_reformatted.trace',
          'IBM_traces/requests_ibm_reformatted.trace',
          'UMass_YouTube_traces/requests_full_youtube_reformatted.trace',
          'UMass_YouTube_traces/requests_youtube_reformatted.trace',
          'Live_VoD_P2P_IPTV_elkhatib/NextSharePC_one_cache_scenario.trace',
          'Live_VoD_P2P_IPTV_elkhatib/NextShareTV_one_cache_scenario.trace'
         ]
    traces.extend(['synthetic/mult_zip_run%i_reformatted.trace' % x for x in range(1, 11)])
    traces.append('synthetic/zip0.8_300k_requests_reformatted.trace')
    traces.append('synthetic/zip0.8_reformatted.trace')

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
