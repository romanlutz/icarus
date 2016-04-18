import csv
import os

from icarus.io.readwrite import read_results

def print_results_full(format):
    for tree in read_results('results%s' % format, format):
        for k in tree[0]:
            print k
        for k in tree[1]:
            print k
        print ''

def print_cache_hit_rates(format, trace=True):
    rates = {}

    for tree in read_results('results%s' % format, format):
        topology_params, trace_params, synthetic_experiment_params, policy_params, strategy, cache_size = determine_parameters(tree)

        rates = assign_results(tree, rates, topology_params, trace_params, synthetic_experiment_params, policy_params, strategy)

    policies = ['ARC', 'LRU', 'KLRU', 'SS', 'DSCA', '2DSCA', 'DSCAAWS', '2DSCAAWS', 'DSCASW', 'DSCAFT', 'DSCAFS', 'ADSCASTK', 'ADSCAATK']

    dict_list = []

    for policy in policies:
        if policy in rates.keys():
            if policy in ['ARC', 'LRU', 'SS']:
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
            elif policy in ['DSCA', '2DSCA', 'DSCAFT']:
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    dict_list.append(('%s %d' % (policy, window_size), rates[policy][window_size]))
            elif policy in ['DSCAAWS', '2DSCAAWS']:
                periods = rates[policy].keys()
                periods.sort()
                for period in periods:
                    hypo_As = rates[policy][period].keys()
                    hypo_As.sort()
                    for A in hypo_As:
                        hypo_epsilons = rates[policy][period][A].keys()
                        hypo_epsilons.sort()
                        for epsilon in hypo_epsilons:
                            dict_list.append(('%s %d %f %f' % (policy, period, A, epsilon),
                                             rates[policy][period][A][epsilon]))
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


    for result_dict in dict_list:
        print '%s\t' % result_dict[0],
        for desc in result_dict[1]:
            print '%s: %f \t' % (desc, sum(result_dict[1][desc])/len(result_dict[1][desc])),
        print('')

def determine_parameters(tree):
    topology_params = {}
    trace_params = {}
    synthetic_experiment_params = {}
    policy_params = {}
    strategy = None
    cache_size = None

    for k in tree[0]:
        if k[0] == ('workload', 'reqs_file'):
            trace_params['trace'] = k[1]

        elif k[0] == ('workload', 'q'):
            synthetic_experiment_params['q'] = k[1]

        elif k[0] == ('workload', 'alpha'):
            synthetic_experiment_params['alpha'] = k[1]

        elif k[0] == ('strategy', 'name'):
            strategy = k[1]

        elif k[0] == ('topology', 'name'):
            topology_params['name'] = k[1]

        elif k[0] == ('topology', 'n'):
            topology_params['n'] = int(k[1])

        elif k[0] == ('topology', 'k'):
            topology_params['k'] = int(k[1])

        elif k[0] == ('topology', 'h'):
            topology_params['h'] = int(k[1])

        elif k[0] == ('cache_placement', 'network_cache_absolute'):
            if k[1] is not None:
                cache_size = k[1]

        elif k[0] == ('cache_placement', 'network_cache_per_node'):
            if k[1] is not None:
                cache_size = k[1]

        elif k[0] == ('cache_placement', 'network_cache_all_nodes'):
            if k[1] is not None:
                cache_size = k[1]

        elif k[0] == ('cache_policy', 'name'):
            policy_params['policy'] = k[1]

        elif k[0] == ('cache_policy', 'window_size'):
            if k[1] is not None:
                policy_params['window_size'] = int(k[1])

        elif k[0] == ('cache_policy', 'segments'):
            if k[1] is not None:
                policy_params['segments'] = int(k[1])

        elif k[0] == ('cache_policy', 'cached_segments'):
            if k[1] is not None:
                policy_params['cached_segments'] = int(k[1])

        elif k[0] == ('cache_policy', 'subwindows'):
            if k[1] is not None:
                policy_params['subwindows'] = int(k[1])

        elif k[0] == ('cache_policy', 'subwindow_size'):
            if k[1] is not None:
                policy_params['subwindow_size'] = int(k[1])

        elif k[0] == ('cache_policy', 'lru_portion'):
            if k[1] is not None:
                policy_params['lru_portion'] = float(k[1])

        elif k[0] == ('cache_policy', 'hypothesis_check_period'):
            if k[1] is not None:
                policy_params['hypothesis_check_period'] = int(k[1])

        elif k[0] == ('cache_policy', 'hypothesis_check_A'):
            if k[1] is not None:
                policy_params['hypothesis_check_A'] = float(k[1])

        elif k[0] == ('cache_policy', 'hypothesis_check_epsilon'):
            if k[1] is not None:
                policy_params['hypothesis_check_epsilon'] = float(k[1])

        elif k[0] in [('cache_placement', 'network_cache_per_node'), ('cache_placement', 'network_cache_all_nodes'), ('cache_placement', 'network_cache_absolute')]:
            if k[1] is not None:
                policy_params['cache_size'] = int(k[1])

    return topology_params, trace_params, synthetic_experiment_params, policy_params, strategy, cache_size

def assign_results(tree, rates, topology_params, trace_params, synthetic_experiment_params, policy_params, strategy):

    if trace_params.keys() != []:
        # deterministic trace-driven experiments
        description = trace_params['trace']
    else:
        # synthetic (stationary) experiments
        description = topology_params['name']
        del topology_params['name']
        for param_name in topology_params:
            description += ' %s=%s' % (param_name, topology_params[param_name])
        for param_name in synthetic_experiment_params:
            description += ' %s=%s' % (param_name, synthetic_experiment_params[param_name])
        description += ' %s' % strategy


    for k in tree[1]:
        if k[0] == ('CACHE_HIT_RATIO', 'PER_NODE_CACHE_HIT_RATIO', 1):
            if policy_params['policy'] not in rates.keys():
                rates[policy_params['policy']] = {}
            for param_name in ['window_size', 'segments', 'subwindow_size', 'hypothesis_check_period']:
                if param_name in policy_params and policy_params[param_name] is not None \
                        and param_name not in rates[policy_params['policy']].keys():
                    rates[policy_params['policy']][policy_params[param_name]] = {}
            if 'cached_segments' in policy_params and policy_params['cached_segments'] is not None \
                    and 'cached_segments' not in rates[policy_params['policy']][policy_params['segments']].keys():
                rates[policy_params['policy']][policy_params['segments']][policy_params['cached_segments']] = {}
            if 'subwindows' in policy_params and policy_params['subwindows'] is not None \
                    and 'subwindows' not in rates[policy_params['policy']][policy_params['subwindow_size']].keys():
                rates[policy_params['policy']][policy_params['subwindow_size']][policy_params['subwindows']] = {}
            if 'lru_portion' in policy_params and policy_params['lru_portion'] is not None \
                    and 'lru_portion' not in rates[policy_params['policy']]['window_size'].keys():
                rates[policy_params['policy']][policy_params['window_size']][policy_params['lru_portion']] = {}
            if 'hypothesis_check_A' in policy_params and policy_params['hypothesis_check_A'] is not None \
                    and 'hypothesis_check_A' not in rates[policy_params['policy']][policy_params['hypothesis_check_period']].keys():
                rates[policy_params['policy']][policy_params['hypothesis_check_period']][policy_params['hypothesis_check_A']] = {}
            if 'hypothesis_check_epsilon' in policy_params and policy_params['hypothesis_check_epsilon'] is not None \
                    and 'hypothesis_check_epsilon' not in \
                            rates[policy_params['policy']][policy_params['hypothesis_check_period']][policy_params['hypothesis_check_A']].keys():
                rates[policy_params['policy']][policy_params['hypothesis_check_period']][policy_params['hypothesis_check_A']][policy_params['hypothesis_check_epsilon']] = {}

            if policy_params['policy'] == 'LRU':
                result_location = rates[policy_params['policy']]
            elif policy_params['policy'] == 'KLRU':
                result_location = rates[policy_params['policy']][policy_params['segments']][policy_params['cached_segments']]
            elif policy_params['policy'] == 'ARC':
                result_location = rates[policy_params['policy']]
            elif policy_params['policy'] == 'SS':
                result_location = rates[policy_params['policy']]
            elif policy_params['policy'] == 'DSCA':
                result_location = rates[policy_params['policy']][policy_params['window_size']]
            elif policy_params['policy'] == '2DSCA':
                result_location = rates[policy_params['policy']][policy_params['window_size']]
            elif policy_params['policy'] == 'DSCAAWS':
                result_location = rates[policy_params['policy']][policy_params['hypothesis_check_period']][policy_params['hypothesis_check_A']][policy_params['hypothesis_check_epsilon']]
            elif policy_params['policy'] == '2DSCAAWS':
                result_location = rates[policy_params['policy']][policy_params['hypothesis_check_period']][policy_params['hypothesis_check_A']][policy_params['hypothesis_check_epsilon']]
            elif policy_params['policy'] == 'DSCASW':
                result_location = rates[policy_params['policy']][policy_params['subwindow_size']][policy_params['subwindows']]
            elif policy_params['policy'] == 'DSCAFT':
                result_location = rates[policy_params['policy']][policy_params['window_size']]
            elif policy_params['policy'] == 'DSCAFS':
                result_location = rates[policy_params['policy']][policy_params['window_size']][policy_params['lru_portion']]
            elif policy_params['policy'] == 'ADSCASTK':
                result_location = rates[policy_params['policy']][policy_params['window_size']]
            elif policy_params['policy'] == 'ADSCAATK':
                result_location = rates[policy_params['policy']][policy_params['window_size']]
            else:
                print('error: policy %s unknown' % policy_params['policy'])

            if description in result_location:
                result_location[description].append(k[1])
            else:
                result_location[description] = [k[1]]

    return rates


def create_path_if_necessary(path, dir):
    directories = path.split('/')[1:-1]
    current_path = dir
    for directory in directories:
        current_path += '/' + directory
        if not os.path.isdir(current_path):
            os.makedirs(current_path)