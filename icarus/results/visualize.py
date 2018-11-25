"""Functions for visualizing results on graphs of topologies"""


import os
from textwrap import wrap

import matplotlib as mpl
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import Normalize

from icarus.io.readwrite import read_results
from icarus.models import policy_parameter_usage

__all__ = [
       'draw_stack_deployment',
       'draw_network_load',
       'draw_cache_level_proportions',
       'draw_cache_hit_ratios'
          ]


# Colormap for node stacks
COLORMAP = {'source':    'blue',
            'receiver':  'green',
            'router':    'white',
            'cache':     'red',
            }


def stack_map(topology):
    """Return dict mapping node ID to stack type
    
    Parameters
    ----------
    topology : Topology
        The topology
    
    Returns
    -------
    stack_map : dict
        Dict mapping node to stack. Options are:
        source | receiver | router | cache
    """
    stack = {}
    for v, (name, props) in list(topology.stacks().items()):
        if name == 'router':
            cache = False
            if 'cache_size' in props and props['cache_size'] > 0:
                cache = True
            elif cache:
                name = 'cache'
            else:
                name = 'router'
        stack[v] = name
    return stack


def draw_stack_deployment(topology, filename, plotdir):
    """Draw a topology with different node colors according to stack
    
    Parameters
    ----------
    topology : Topology
        The topology to draw
    plotdir : string
        The directory onto which draw plots
    filename : string
        The name of the image file to save
    """
    stack = stack_map(topology)
    node_color = [COLORMAP[stack[v]] for v in topology.nodes()]
    plt.figure()
    nx.draw_graphviz(topology, node_color=node_color, with_labels=False)
    plt.savefig(os.path.join(plotdir, filename), bbox_inches='tight', format='pdf')
    

def draw_network_load(topology, result, filename, plotdir):
    """Draw topology with node colors according to stack and node size and link
    color according to server/cache hits and link loads.
    
    Nodes are colored according to COLORMAP. Edge are colored on a blue-red
    scale where blue means min link load and red means max link load.
    Sources and caches have variable size proportional to their hit ratios.
    
    Parameters
    ----------
    topology : Topology
        The topology to draw
    result : Tree
        The tree representing the specific experiment result from which metric
        are read
    plotdir : string
        The directory onto which draw plots
    filename : string
        The name of the image file to save
    """
    stack = stack_map(topology)
    node_color = [COLORMAP[stack[v]] for v in topology.nodes()]
    node_min = 50
    node_max = 600
    hits = result['CACHE_HIT_RATIO']['PER_NODE_CACHE_HIT_RATIO'].copy()
    hits.update(result['CACHE_HIT_RATIO']['PER_NODE_SERVER_HIT_RATIO'])
    hits = np.array([hits[v] if v in hits else 0 for v in topology.nodes()])
    min_hits = np.min(hits)
    max_hits = np.max(hits)
    hits = node_min +  (node_max - node_min)*(hits - min_hits)/(max_hits - min_hits)
    link_load = result['LINK_LOAD']['PER_LINK_INTERNAL'].copy()
    link_load.update(result['LINK_LOAD']['PER_LINK_EXTERNAL'])
    link_load = [link_load[e] if e in link_load else 0 for e in topology.edges()]
    plt.figure()
    nx.draw_graphviz(topology, node_color=node_color, node_size=hits, 
                     width=2.0,
                     edge_color=link_load, 
                     edge_cmap=mpl.colors.LinearSegmentedColormap.from_list('bluered',['blue','red']),
                     with_labels=False)
    plt.savefig(plt.savefig(os.path.join(plotdir, filename), bbox_inches='tight'))

def draw_cache_level_proportions(plotdir, filename, format):
    from .output_results import determine_parameters

    result = read_results('%s%s' % (filename, format), format)
    for tree in result:
        trace, policy, cache_size, window_size, segments, cached_segments, subwindows, subwindow_size, lru_portion, \
           hypothesis_check_period, hypothesis_check_A, hypothesis_check_epsilon = determine_parameters(tree)
        print(trace, policy)
        param_names = ['window_size', 'subwindows', 'subwindow_size', 'segments', 'cached_segments', 'lru_portion']
        params = [window_size, subwindows, subwindow_size, segments, cached_segments, lru_portion]
        if policy in ['ARC', 'DSCA', 'DSCASW', 'ADSCASTK', 'ADSCAATK']:
            lru_sizes = {}
            lfu_sizes = {}
            node_name = -1
            for k in tree[1]:
                if k[0][0] == 'CACHE_LEVEL_PROPORTIONS':
                    node_name = k[0][1].split(':')[0]
                    if 'LRU' in k[0][1]:
                        lru_sizes = k[1]
                    elif 'LFU' in k[0][1]:
                        lfu_sizes = k[1]

            if node_name != -1 and lru_sizes != [] and lfu_sizes != []:

                filename = trace[10:-6] + '/' + policy + '_cache_size=' + str(cache_size)
                title = 'Cache Level Proportions for %s with cache size=%i' % (policy, cache_size)
                # use only policy-relevant parameters in the filename
                used_parameters = policy_parameter_usage(policy)
                for index, param in enumerate(params):
                    if used_parameters[param_names[index]]:
                        filename += '_%s=%s' % (param_names[index], str(param))
                        pretty_param_name = param_names[index].replace('_', ' ')
                        title += ', %s=%s' % (pretty_param_name, str(param))

                # include node name in filename if the method is used in a multi-cache scenario
                filename += '_' + node_name.replace(' ', '') + '.pdf'

                path = os.path.join(plotdir, filename)

                # ensure the path exists and create it if necessary
                directories = path.split('/')[1:-1]
                current_path = plotdir
                for directory in directories:
                    current_path += '/' + directory
                    if not os.path.isdir(current_path):
                        os.makedirs(current_path)

                pdf=PdfPages(path)
                fig = plt.figure()
                [lru_indices, lru_sizes] = [list(t) for t in zip(*lru_sizes)]
                [lfu_indices, lfu_sizes] = [list(t) for t in zip(*lfu_sizes)]
                p1 = plt.plot(lru_indices, lru_sizes, '-', linewidth=2, color='r')
                p2 = plt.plot(lfu_indices, lfu_sizes, '-', linewidth=2, color='b')
                plt.legend((p1[0], p2[0]), ('LRU', 'LFU'))
                plt.xlabel('incoming elements')
                plt.ylabel('cache size')
                plt.title("\n".join(wrap(title, 60)))

                pdf.savefig(fig)
                pdf.close()
                plt.close()


def draw_cache_hit_ratios(results, data_desc):
    filename = '%s.png' % data_desc
    filename = filename.rpartition('/')[2]
    plotdir = 'plots/cache_hit_rates/'

    path = os.path.join(plotdir, filename)
    print(path)

    # ensure the path exists and create it if necessary
    directories = path.split('/')[1:-1]
    current_path = plotdir
    for directory in directories:
        current_path += '/' + directory
        if not os.path.isdir(current_path):
            os.makedirs(current_path)

    policy_ticks = []
    strategy_ticks = [0] * 5
    values = []
    group_index = -1
    results = results[::-1]

    for i, (desc, result) in enumerate(results, start=0):
        if i % 5 == 0:
            group_index += 1
            values.append([])
            policy_ticks.append(' '.join(desc.partition(' +')[0].split(' ')[:2]))
        strategy_ticks[i % 5] = desc.partition('+ ')[2]
        values[group_index].append(result)

    plt.pcolor(np.array(values), cmap=plt.cm.seismic)

    plt.xticks(np.linspace(0.5, len(strategy_ticks) - 0.5, len(strategy_ticks)), strategy_ticks, rotation=90)
    plt.yticks(np.linspace(0.5, len(policy_ticks) - 0.5, len(policy_ticks)), policy_ticks)
    plt.xlabel('strategy')
    plt.ylabel('policy')
    plt.gca().set_aspect('equal')
    plt.gcf().tight_layout()
    plt.colorbar()
    plt.savefig(path, bbox_inches='tight')
    plt.close()


def create_result_evolution_plots(plot_rates, data_desc, metric_name, param_names, policy_order):
    filename = '%s.png' % data_desc
    plotdir = 'plots/cache_hit_rate_evolution/'

    path = os.path.join(plotdir, filename)
    print(path)

    # ensure the path exists and create it if necessary
    directories = path.split('/')[1:-1]
    current_path = plotdir
    for directory in directories:
        current_path += '/' + directory
        if not os.path.isdir(current_path):
            os.makedirs(current_path)

    # plot values per parameter
    plots = []
    policy_names = []

    policy_colors = {'ARC': 0, 'LRU': 1, 'KLRU': 1, 'DSCA': 2, '2DSCA': 3, 'DSCAAWS': 4, '2DSCAAWS': 5}
    policy_linestyles = {'1': 'solid', '4': 'dashed', '16': 'dashdot', '64': 'dotted', '(2,1)': 'dashed'}
    policy_markers = {'ARC': '>', 'LRU': '<', 'KLRU': '^', 'DSCA': 'v', '2DSCA': 'o', 'DSCAAWS': '+', '2DSCAAWS': '8'}

    cmap = plt.get_cmap('nipy_spectral')
    norm = Normalize(0, len(policy_colors) - 1)

    fig = plt.figure(figsize=(13, 13), dpi=800)

    subplot_index = 0
    parameter1_values = list(plot_rates.keys())
    parameter1_values.sort()
    for param1 in parameter1_values:
        parameter2_values = list(plot_rates[param1].keys())
        parameter2_values.sort()
        for param2 in parameter2_values:
            plots = []
            subplot_index += 1
            ax = fig.add_subplot(len(parameter1_values),len(parameter2_values),subplot_index)

            occurring_policies = list(plot_rates[param1][param2].keys())
            occurring_policies = sorted(occurring_policies,
                                        key=lambda policy: 0 if ' ' not in policy or '(' in policy else int(
                                            policy.split(' ')[1]))
            occurring_policies = sorted(occurring_policies,
                                        key=lambda policy: policy_order.index(policy.partition(' ')[0]))

            min_value = 1
            max_value = 0
            parameter3_values = []
            for policy in occurring_policies:
                policy_names.append(policy)
                parameter3_values = list(plot_rates[param1][param2][policy].keys())
                parameter3_values.sort()

                values = []

                for param3 in parameter3_values:
                    values.append(plot_rates[param1][param2][policy][param3])

                if max(values) > max_value:
                    max_value = max(values)
                if min(values) < min_value:
                    min_value = min(values)

                plots.append(ax.plot(parameter3_values, values, color=cmap(norm(policy_colors[policy.partition(' ')[0]])), linestyle='solid' if ' ' not in policy else policy_linestyles[policy.split(' ')[1]], marker=policy_markers[policy.partition(' ')[0]]))

            ax.set_xlabel(param_names[2])
            ax.set_ylabel(metric_name)
            #ax.set_xscale('log')
            ax.set_xlim([min(parameter3_values) - 0.05 * (max(parameter3_values) - min(parameter3_values)), max(parameter3_values) + 0.05 * (max(parameter3_values) - min(parameter3_values))])
            ax.set_ylim([max([0, min_value - 0.05 * (max_value - min_value)]), max_value + 0.05 * (max_value - min_value)])
            ax.set_title('%s=%s, %s=%s' % (param_names[0], str(param1), param_names[1], str(param2)))
    plt.figlegend(tuple([x[0] for x in plots]), tuple([' '.join(n.split(' ')[:2]) for n in policy_names]),
                  bbox_to_anchor=(1.18, .5), loc='right', borderaxespad=0.)

    plt.gcf().tight_layout()
    plt.savefig(path, bbox_inches='tight')
    plt.close()
