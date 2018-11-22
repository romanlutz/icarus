import csv
import os
from collections import defaultdict
from icarus.io.readwrite import read_results
from icarus.results.visualize import draw_cache_hit_ratios, create_result_evolution_plots

def print_results_full(filename, format):
    for tree in read_results('%s%s' % (filename, format), format):
        for k in tree[0]:
            print k
        for k in tree[1]:
            print k
        print ''

def provide_result_dictionary(filename, format, goal_tuple):
    f = lambda: defaultdict(f)
    rates = defaultdict(f)
    descriptions = []

    for tree in read_results('%s%s' % (filename, format), format):
        topology_params, trace_params, synthetic_experiment_params, policy_params, strategy, cache_size = determine_parameters(
            tree)

        rates, descriptions = assign_results(goal_tuple, tree, rates, descriptions, topology_params, trace_params,
                                             synthetic_experiment_params, policy_params, strategy)

    policies = ['ARC', 'LRU', 'KLRU', 'SS', 'DSCA', '2DSCA', 'DSCAAWS', '2DSCAAWS', 'DSCASW', 'DSCAFT', 'DSCAFS',
                'ADSCASTK', 'ADSCAATK']
    strategies = ['LCE', 'LCD', 'CL4M', 'PROB_CACHE', 'RAND_CHOICE']

    dict_list = []

    for policy in policies:
        if policy in rates.keys():
            if policy in ['ARC', 'LRU', 'SS']:
                for strategy in strategies:
                    dict_list.append(put_results_if_available('%s + %s' % (policy, strategy), rates[policy], strategy))
            elif policy == 'KLRU':
                segment_values = rates[policy].keys()
                segment_values.sort()
                for segment_value in segment_values:
                    cached_segment_values = rates[policy][segment_value].keys()
                    cached_segment_values.sort()
                    for cached_segment_value in cached_segment_values:
                        for strategy in strategies:
                            dict_list.append(put_results_if_available(
                                'KLRU (%d,%d) + %s' % (segment_value, cached_segment_value, strategy),
                                rates[policy][segment_value][cached_segment_value], strategy))
            elif policy in ['DSCA', '2DSCA', 'DSCAFT']:
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    for strategy in strategies:
                        dict_list.append(put_results_if_available('%s %d + %s' % (policy, window_size, strategy),
                                                                  rates[policy][window_size], strategy))
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
                            for strategy in strategies:
                                dict_list.append(put_results_if_available(
                                    '%s %d %f %f + %s' % (policy, period, A, epsilon, strategy),
                                    rates[policy][period][A][epsilon], strategy))
            elif policy == 'DSCASW':
                subwindow_sizes = rates[policy].keys()
                subwindow_sizes.sort()
                for subwindow_size in subwindow_sizes:
                    subwindows_values = rates[policy][subwindow_size].keys()
                    subwindows_values.sort()
                    for subwindows in subwindows_values:
                        for strategy in strategies:
                            dict_list.append(
                                put_results_if_available('DSCASW (%d %d) + %s' % (subwindow_size, subwindows, strategy),
                                                         rates[policy][subwindow_size][subwindows], strategy))
            elif policy == 'DSCAFS':
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    lru_portions = rates[policy][window_size].keys()
                    lru_portions.sort()
                    for lru_portion in lru_portions:
                        for strategy in strategies:
                            dict_list.append(
                                put_results_if_available('DSCAFS (%d %f) + %s' % (window_size, lru_portion, strategy),
                                                         rates[policy][window_size][lru_portion], strategy))
            elif policy in ['ADSCASTK', 'ADSCAATK']:
                window_sizes = rates[policy].keys()
                window_sizes.sort()
                for window_size in window_sizes:
                    for strategy in strategies:
                        dict_list.append(put_results_if_available('%s %d + %s' % (policy, window_size, strategy),
                                                                  rates[policy][window_size], strategy))

    descriptions.sort()

    return descriptions, dict_list


def print_cache_hit_rates(filename, format, goal_tuple, plot=False):
    descriptions, dict_list = provide_result_dictionary(filename, format, goal_tuple)

    print '\t',
    for description in descriptions:
        print '%s\t' % description,
    print('')

    dict_list_per_desc = defaultdict(list)

    for result_dict in dict_list:
        if result_dict[1] != 'fail':
            print '%s\t' % result_dict[0],

            for desc in descriptions:
                if desc in result_dict[1]:
                    average_hit_rate = sum(result_dict[1][desc])/len(result_dict[1][desc])
                    print '%f \t' % average_hit_rate,
                    dict_list_per_desc[desc].append((result_dict[0], average_hit_rate))
                else:
                    print 'fail\t'
                    dict_list_per_desc[desc].append('fail')
            print('')

    if plot:
        for desc in dict_list_per_desc:
            draw_cache_hit_ratios(dict_list_per_desc[desc], desc)

def put_results_if_available(desc, dict, strategy):
    if strategy not in dict:
        dict[strategy] = 'fail'
    return (desc, dict[strategy])

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

        elif k[0] == ('workload', 'weights'):
            trace_params['weights'] = k[1].rpartition('/')[2]

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

def assign_results(goal_tuple, tree, rates, descriptions, topology_params, trace_params, synthetic_experiment_params, policy_params, strategy):

    if trace_params.keys() != []:
        # deterministic trace-driven experiments
        description = '%s + %s' % (trace_params['trace'], trace_params['weights'])
    else:
        # synthetic (stationary) experiments
        description = topology_params['name']
        del topology_params['name']
        for param_name in topology_params:
            description += ' %s=%s' % (param_name, topology_params[param_name])
        for param_name in synthetic_experiment_params:
            description += ' %s=%s' % (param_name, synthetic_experiment_params[param_name])

    for k in tree[1]:
        if k[0] == goal_tuple:
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
                print('error: policy %s or strategy %s unknown' % (policy_params['policy'], strategy))

            if description in result_location:
                result_location[strategy][description].append(k[1])
            else:
                result_location[strategy][description] = [k[1]]

            if description not in descriptions:
                descriptions.append(description)

    return rates, descriptions


def create_path_if_necessary(path, dir):
    directories = path.split('/')[1:-1]
    current_path = dir
    for directory in directories:
        current_path += '/' + directory
        if not os.path.isdir(current_path):
            os.makedirs(current_path)


def generate_result_evolution_plots(trace_abbreviation, percentages, weights, cache_sizes, combinations):
    # gather all data in a dictionary
    f = lambda: defaultdict(f)
    rates = defaultdict(f)

    def create_or_extend_list(location, new_list):
        if type(location) == list:
            location.extend(new_list)
        else:
            location = new_list

        return location

    for weighted_or_not in ['weighted', 'unweighted']:
        for cache_size in cache_sizes:
            file_name = 'results-%s-%s-c%d' % (weighted_or_not, trace_abbreviation, cache_size)
            format = '.spickle'

            try:
                for metric_description in combinations:
                    goal_tuple = ('CACHE_HIT_RATIO', combinations[metric_description])

                    descriptions, dict_list = provide_result_dictionary(file_name, format, goal_tuple)

                    for result_dict in dict_list:
                        if result_dict[1] != 'fail':
                            policy = result_dict[0].rpartition(' +')[0]

                            for desc in descriptions:
                                # detect weight
                                weight = None
                                for w in weights[1:]:
                                    if 'w%d' % w in desc:
                                        weight = w
                                if 'UNIFORM' in desc:
                                    weight = 1
                                if weight is None:
                                    print 'error: weight could not be detected'

                                # detect percentage
                                percentage = None
                                if weight == 1:
                                    percentage = 'all'
                                else:
                                    for p in percentages[1:]:
                                        if 'p%f' % p in desc:
                                            percentage = p
                                if percentage is None:
                                    print 'error: percentage could not be detected'

                                # add results to dictionary
                                if desc in result_dict[1]:
                                    # this is a list with one entry
                                    # the repetitions are handled through different weight files
                                    cache_hit_rate_list = result_dict[1][desc]

                                    if percentage == 'all' and weight == 1:
                                        # add it to all percentages for weight 1
                                        # add it to all weights with percentage 0
                                        for percentage in percentages:
                                            rates[policy][cache_size][weight][percentage][metric_description] = create_or_extend_list(rates[policy][cache_size][weight][percentage][metric_description], cache_hit_rate_list)
                                        for weight in weights:
                                            rates[policy][cache_size][weight][0][metric_description] = create_or_extend_list(rates[policy][cache_size][weight][0][metric_description], cache_hit_rate_list)
                                    else:
                                        rates[policy][cache_size][weight][percentage][metric_description] = create_or_extend_list(rates[policy][cache_size][weight][percentage][metric_description], cache_hit_rate_list)
            except:
                print 'error: file %s not available' % file_name

    policies = ['ARC', 'LRU', 'KLRU', 'SS', 'DSCA', '2DSCA', 'DSCAAWS', '2DSCAAWS', 'DSCASW', 'DSCAFT', 'DSCAFS', 'ADSCASTK', 'ADSCAATK']

    for metric_description in combinations:
        def average(lst):
            return sum(lst) / len(lst)

        def normalize(metric_name, percentage, weight, value):
            if metric_name == 'weighted average hit rate' and percentage > 0 and weight > 1:
                return value / (weight + 1)
            else:
                return value

        # plot by percentages
        plot_rates = defaultdict(f)
        for cache_size in cache_sizes:
            for weight in weights:
                # the data for the plot is the values of each policy over the different percentages

                for policy in rates:
                    for percentage in percentages:
                        if cache_size in rates[policy] and weight in rates[policy][cache_size] and \
                            percentage in rates[policy][cache_size][weight] and \
                            metric_description in rates[policy][cache_size][weight][percentage]:

                            plot_rates[cache_size][weight][policy][percentage] = normalize(metric_description, percentage, weight, average(rates[policy][cache_size][weight][percentage][metric_description]))

        create_result_evolution_plots(plot_rates, '%s-%s-%s' % (
            trace_abbreviation, metric_description.replace(' ', '-'), 'by-percentage'), metric_description,
                                      ['cache size', 'weight', 'percentage of weighted contents'],
                                      policy_order=policies)
        # plot by weights
        plot_rates = defaultdict(f)
        for cache_size in cache_sizes:
            for percentage in percentages:
                # the data for the plot is the values of each policy over the different weights

                for policy in rates:
                    for weight in weights:
                        if cache_size in rates[policy] and weight in rates[policy][cache_size] and \
                                        percentage in rates[policy][cache_size][weight] and \
                                        metric_description in rates[policy][cache_size][weight][percentage]:

                            plot_rates[cache_size][percentage][policy][weight] = normalize(metric_description, percentage, weight, average(rates[policy][cache_size][weight][percentage][metric_description]))

        create_result_evolution_plots(plot_rates, '%s-%s-%s' % (
            trace_abbreviation, metric_description.replace(' ', '-'), 'by-weight'), metric_description,
                                      ['cache size', 'percentage', 'weight'],
                                      policy_order=policies)

        # plot by cache sizes
        plot_rates = defaultdict(f)
        for weight in weights:
            for percentage in percentages:
                # the data for the plot is the values of each policy over the different cache sizes

                for policy in rates:
                    for cache_size in cache_sizes:
                        if cache_size in rates[policy] and weight in rates[policy][cache_size] and \
                                        percentage in rates[policy][cache_size][weight] and \
                                        metric_description in rates[policy][cache_size][weight][percentage]:
                            plot_rates[percentage][weight][policy][cache_size] = normalize(metric_description, percentage, weight, average(rates[policy][cache_size][weight][percentage][metric_description]))

        create_result_evolution_plots(plot_rates, '%s-%s-%s' % (
            trace_abbreviation, metric_description.replace(' ', '-'), 'by-cache-size'), metric_description,
                                      ['percentage', 'weight', 'cache size'],
                                      policy_order=policies)

