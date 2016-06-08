"""This module contains all configuration information used to run simulations.

Overview
========

This reference configuration file is divided into two parts.
The first part lists generic simulation parameters such as number of processes
to use, logging configuration and so on.
The second part builds an "experiment queue", i.e. a queue of configuration
parameters each representing one single experiment.

Each element of the queue must be an instance of the icarus.util.Tree class,
which is an object modelling a tree of hierarchically organized configuration
parameters. Alternatively nested dictionaries can be used instead of trees. In
this case Icarus will convert them to trees at runtime. It is however suggested
to use Tree objects because they provide methods that simplify the definition
of experiments.
"""
from multiprocessing import cpu_count
from collections import deque
import csv
from icarus.util import Tree
import itertools


def append_default(cache_policy_parameters, window_size=False, subwindows=False, subwindow_size=False, monitored=False,
                   segments=False, cached_segments=False, lru_portion=False, hypothesis_check_period=False,
                   hypothesis_check_A=False, hypothesis_check_epsilon=False):
    variables = ['window_size', 'subwindows', 'subwindow_size', 'monitored', 'segments', 'cached_segments',
                 'lru_portion', 'hypothesis_check_period', 'hypothesis_check_A', 'hypothesis_check_epsilon']
    for variable in variables:
        if eval(variable):
            cache_policy_parameters[variable].append(None)


############################## GENERAL SETTINGS ##############################

# Level of logging output
# Available options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = 'INFO'

# If True, executes simulations in parallel using multiple processes
# to take advantage of multicore CPUs
PARALLEL_EXECUTION = True

# Number of processes used to run simulations in parallel.
# This option is ignored if PARALLEL_EXECUTION = False
N_PROCESSES = cpu_count()

# Granularity of caching.
# Currently, only OBJECT is supported
CACHING_GRANULARITY = 'OBJECT'

# Format in which results are saved.
# Result readers and writers are located in module ./icarus/results/readwrite.py
# Currently only PICKLE is supported 
RESULTS_FORMAT = 'SPICKLE'

########################## EXPERIMENTS CONFIGURATION ##########################

# whether the experiments will be based on synthetic data or traces
# some later steps are relevant only for synthetic or trace-driven experiments
SYNTHETIC_EXPERIMENT = False
TRACE_DRIVEN_EXPERIMENTS = False
DETERMINISTIC_TRACE_DRIVEN_EXPERIMENTS = True

# List of metrics to be measured in the experiments
# The implementation of data collectors are located in ./icarus/execution/collectors.py
# Remove collectors not needed
DATA_COLLECTORS = {
    'CACHE_HIT_RATIO': {'content_hits': False, 'per_node': True},  # Measure cache hit ratio
    # 'LATENCY': {},           # Measure request and response latency (based on static link delays)
    # 'LINK_LOAD': {},         # Measure link loads
    # 'PATH_STRETCH': {},      # Measure path stretch
    # 'CACHE_LEVEL_PROPORTIONS': {},
    # 'WINDOW_SIZE': {} # only for adaptive window size cache policies / not usable yet
}

# The size of network cache can be set as a fraction of content population in three ways:
# 1. define a fraction of the number of contents which is assigned to every node, set NETWORK_CACHE_PER_NODE
# 2. define a fraction of the number of contents which is distributed over all nodes, set NETWORK_CACHE_ALL_NODES
# 3. define an absolute number as the total cache for the whole network
NETWORK_CACHE_PER_NODE = None  # 0.01
NETWORK_CACHE_ALL_NODES = None
NETWORK_CACHE_ABSOLUTE = 8000
if NETWORK_CACHE_PER_NODE is not None:
    NETWORK_CACHE = NETWORK_CACHE_PER_NODE
elif NETWORK_CACHE_ALL_NODES is not None:
    NETWORK_CACHE = NETWORK_CACHE_ALL_NODES
elif NETWORK_CACHE_ABSOLUTE is not None:
    NETWORK_CACHE = NETWORK_CACHE_ABSOLUTE

# List of caching and routing strategies
# The code is located in ./icarus/models/strategy.py
# Remove strategies not needed
STRATEGIES = [
    'LCE',  # Leave Copy Everywhere
    # 'NO_CACHE',        # No caching, shortest-path routing
    # 'HR_SYMM',         # Symmetric hash-routing
    # 'HR_ASYMM',        # Asymmetric hash-routing
    # 'HR_MULTICAST',    # Multicast hash-routing
    # 'HR_HYBRID_AM',    # Hybrid Asymm-Multicast hash-routing
    # 'HR_HYBRID_SM',    # Hybrid Symm-Multicast hash-routing
    # 'CL4M',            # Cache less for more
    # 'PROB_CACHE',      # ProbCache
    # 'LCD',             # Leave Copy Down
    # 'RAND_CHOICE',     # Random choice: cache in one random cache on path
    # 'RAND_BERNOULLI',  # Random Bernoulli: cache randomly in caches on path
]

# Cache eviction policy
CACHE_POLICY = []
CACHE_POLICY_PARAMETERS = {'window_size': [], 'subwindows': [], 'subwindow_size': [], 'monitored': [],
                           'segments': [], 'cached_segments': [], 'lru_portion': [], 'hypothesis_check_period': [],
                           'hypothesis_check_A': [], 'hypothesis_check_epsilon': []}

MONITORED_DEFAULT = 2.0

use_SS = False
use_DSCA = True
use_2DSCA = True
use_DSCAAWS = True
use_2DSCAAWS = True
use_DSCASW = False
use_DSCAFT = False
use_DSCAFS = False
use_ADSCASTK = False
use_ADSCAATK = False
use_ARC = True
use_LRU = True
use_KLRU = True

if use_SS:
    CACHE_POLICY.append('SS')
    CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
    append_default(CACHE_POLICY_PARAMETERS, window_size=True, subwindows=True, subwindow_size=True,
                   segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True)

if use_DSCA:
    for window_size in [4, 16, 64]:
        CACHE_POLICY.append('DSCA')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_2DSCA:
    for window_size in [4, 16, 64]:
        CACHE_POLICY.append('2DSCA')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_DSCAAWS:
    for hypothesis_check_period in [1, 4, 16]:
        for hypothesis_check_A in [0.33]:
            for hypothesis_check_epsilon in [0.005]:
                CACHE_POLICY.append('DSCAAWS')
                CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
                CACHE_POLICY_PARAMETERS['hypothesis_check_period'].append(hypothesis_check_period)
                CACHE_POLICY_PARAMETERS['hypothesis_check_A'].append(hypothesis_check_A)
                CACHE_POLICY_PARAMETERS['hypothesis_check_epsilon'].append(hypothesis_check_epsilon)
                append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                               cached_segments=True, lru_portion=True, window_size=True)

if use_2DSCAAWS:
    for hypothesis_check_period in [1, 4, 16]:
        for hypothesis_check_A in [0.33]:
            for hypothesis_check_epsilon in [0.005]:
                CACHE_POLICY.append('2DSCAAWS')
                CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
                CACHE_POLICY_PARAMETERS['hypothesis_check_period'].append(hypothesis_check_period)
                CACHE_POLICY_PARAMETERS['hypothesis_check_A'].append(hypothesis_check_A)
                CACHE_POLICY_PARAMETERS['hypothesis_check_epsilon'].append(hypothesis_check_epsilon)
                append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                               cached_segments=True, lru_portion=True, window_size=True)

if use_DSCASW:
    for subwindow_size in [1, 4, 16]:
        for subwindows in range(2, 11, 2):
            CACHE_POLICY.append('DSCASW')
            CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
            CACHE_POLICY_PARAMETERS['subwindows'].append(subwindows)
            CACHE_POLICY_PARAMETERS['subwindow_size'].append(subwindow_size)
            append_default(CACHE_POLICY_PARAMETERS, window_size=True, segments=True, cached_segments=True,
                           lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)

if use_DSCAFT:
    for window_size in [4, 16, 64]:
        CACHE_POLICY.append('DSCAFT')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        # the threshold is used as specified through the default value
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_DSCAFS:
    for window_size in [4, 16, 64]:
        for lru_portion in [0.1, 0.25, 0.5, 0.75, 0.9]:
            CACHE_POLICY.append('DSCAFS')
            CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
            CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
            CACHE_POLICY_PARAMETERS['lru_portion'].append(lru_portion)
            append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                           cached_segments=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)

if use_ADSCASTK:
    for window_size in [4, 16, 64]:
        CACHE_POLICY.append('ADSCASTK')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_ADSCAATK:
    for window_size in [4, 16, 64]:
        CACHE_POLICY.append('ADSCAATK')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_ARC:
    CACHE_POLICY.append('ARC')
    append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True, subwindow_size=True,
                   segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True)

if use_LRU:
    CACHE_POLICY.append('LRU')
    append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True, subwindow_size=True,
                   segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True)

if use_KLRU:
    for segments in [2]:
        for cached_segments in [1]:
            CACHE_POLICY.append('KLRU')
            CACHE_POLICY_PARAMETERS['cached_segments'].append(cached_segments)
            CACHE_POLICY_PARAMETERS['segments'].append(segments)
            append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True,
                           subwindow_size=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)

# Instantiate experiment queue
EXPERIMENT_QUEUE = deque()

if SYNTHETIC_EXPERIMENT:
    # Number of times each experiment is replicated
    # This is necessary for extracting confidence interval of selected metrics
    N_REPLICATIONS = 1

    # number of contents
    N_CONTENTS = 100000

    # number of requests = warmup period + number of measured requests
    N_REQUESTS = 10000000

    # Mandelbrot-Zipf alpha and q parameters for non-trace-driven simulation
    ALPHA = [0.5, 0.75, 1]
    Q = [0, 5, 50]

    # random seeds given to workload creating library
    seeds = range(N_REPLICATIONS)

    # List of topologies tested
    # Topology implementations are located in ./icarus/scenarios/topology.py
    # Remove topologies not needed
    TOPOLOGIES = {
        #'PATH': {'n': [7]},
        #'TREE': {'k': [2], 'h': [4]},
        'GEANT': {},
        #'GEANT_2': {},
        #'WIDE': {},
        #'GARR': {},
        #'GARR_2': {},
        # 'TISCALI': {},
        # 'TISCALI_2': {}
    }

    # Create experiments multiplexing all desired parameters
    for rep in range(N_REPLICATIONS):
        for (alpha, q) in list(itertools.product(ALPHA, Q)):
            for strategy in STRATEGIES:
                for topology in TOPOLOGIES:
                    param_names = TOPOLOGIES[topology].keys()
                    no_params = param_names == []
                    if not no_params:
                        topology_param_combinations = list(
                            itertools.product(*[TOPOLOGIES[topology][param_name] for param_name in param_names]))
                    else:
                        topology_param_combinations = [None]

                    for topology_configuration_index, topology_configuration in enumerate(topology_param_combinations):
                        for cache_policy_index, cache_policy in enumerate(CACHE_POLICY):
                            experiment = Tree()
                            experiment['workload'] = {'name': 'STATIONARY',
                                                      'n_contents': N_CONTENTS,
                                                      'n_warmup': N_CONTENTS,
                                                      'n_measured': N_REQUESTS - N_CONTENTS,
                                                      'weights': 'UNIFORM',
                                                      'seed': seeds[rep]
                                                      }
                            experiment['cache_placement']['name'] = 'UNIFORM'
                            experiment['content_placement']['name'] = 'UNIFORM'
                            experiment['workload']['alpha'] = alpha
                            experiment['workload']['q'] = q
                            experiment['strategy']['name'] = strategy

                            experiment['topology']['name'] = topology
                            if not no_params:
                                for index, param_name in enumerate(param_names):
                                    experiment['topology'][param_name] = topology_configuration[index]

                            experiment['cache_policy']['name'] = cache_policy
                            for param_name, param_value_list in CACHE_POLICY_PARAMETERS.items():
                                experiment['cache_policy'][param_name] = param_value_list[cache_policy_index]
                            experiment['cache_placement']['network_cache_per_node'] = NETWORK_CACHE_PER_NODE
                            experiment['cache_placement']['network_cache_all_nodes'] = NETWORK_CACHE_ALL_NODES
                            experiment['cache_placement']['network_cache_absolute'] = NETWORK_CACHE_ABSOLUTE
                            experiment[
                                'desc'] = "topology: %s, Mandelbrot-Zipf: alpha=%f, q=%f, strategy: %s, cache policy: %s" \
                                          % (topology, alpha, q, strategy, cache_policy)

                            EXPERIMENT_QUEUE.append(experiment)


if TRACE_DRIVEN_EXPERIMENTS:
    # if running a trace-driven simulation, REQ_FILE is the path to the trace file
    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 1
        for line in csv_reader:
            if i == 9:
                traces.append((line[0], int(line[1]), line[2]))
            i += 1

    # List of topologies tested
    # Topology implementations are located in ./icarus/scenarios/topology.py
    # Remove topologies not needed
    TOPOLOGIES = {
        #'PATH': {'n': [7]},
        #'TREE': {'k': [2], 'h': [4]},
        'GEANT': {},
        #'GEANT_2': {},
        #'WIDE': {},
        #'GARR': {},
        #'GARR_2': {},
        # 'TISCALI': {},
        # 'TISCALI_2': {}
    }

    # the warmup phase in which cache hits and misses are not counted
    WARMUP = 100000

    # Create experiments multiplexing all desired parameters
    for trace_name, N_REQUESTS, weights in traces:
        for strategy in STRATEGIES:
            for topology in TOPOLOGIES:
                param_names = TOPOLOGIES[topology].keys()
                no_params = param_names == []
                if not no_params:
                    topology_param_combinations = list(
                        itertools.product(*[TOPOLOGIES[topology][param_name] for param_name in param_names]))
                else:
                    topology_param_combinations = [None]

                for topology_configuration_index, topology_configuration in enumerate(topology_param_combinations):
                    for cache_policy_index, cache_policy in enumerate(CACHE_POLICY):
                        experiment = Tree()
                        experiment['workload'] = {'name': 'TRACE_DRIVEN',
                                                  'n_warmup': WARMUP,
                                                  'n_measured': N_REQUESTS - WARMUP,
                                                  'reqs_file': 'resources/' + trace_name,
                                                  'weights': weights
                                                  }
                        experiment['cache_placement']['name'] = 'UNIFORM'
                        experiment['content_placement']['name'] = 'UNIFORM'
                        experiment['strategy']['name'] = strategy

                        experiment['topology']['name'] = topology
                        if not no_params:
                            for index, param_name in enumerate(param_names):
                                experiment['topology'][param_name] = topology_configuration[index]

                        experiment['cache_policy']['name'] = cache_policy
                        for param_name, param_value_list in CACHE_POLICY_PARAMETERS.items():
                            experiment['cache_policy'][param_name] = param_value_list[cache_policy_index]
                        experiment['cache_placement']['network_cache_per_node'] = NETWORK_CACHE_PER_NODE
                        experiment['cache_placement']['network_cache_all_nodes'] = NETWORK_CACHE_ALL_NODES
                        experiment['cache_placement']['network_cache_absolute'] = NETWORK_CACHE_ABSOLUTE
                        experiment[
                            'desc'] = "trace: %s, strategy: %s, cache policy: %s" \
                                      % (trace_name, strategy, cache_policy)

                        EXPERIMENT_QUEUE.append(experiment)

# deterministic trace driven simulation configuration:
if DETERMINISTIC_TRACE_DRIVEN_EXPERIMENTS:
    # if running a trace-driven simulation, REQ_FILE is the path to the trace file
    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 1
        for line in csv_reader:
            if i == 94:#i <= 31 and i >= 2:
                traces.append((line[0], int(line[1]), line[2]))
            i += 1

    # List of topologies tested
    # Topology implementations are located in ./icarus/scenarios/topology.py
    # Remove topologies not needed
    TOPOLOGIES = {
        'PATH': {'n': [3]},
        #'TREE': {'k': [2, 2, 2, 2, 4, 4], 'h': [2, 3, 4, 5, 2, 3]},
        #'GEANT': {},
        #'GEANT_2': {},
        #'WIDE': {},
        #'GARR': {},
        #'GARR_2': {},
        #'TISCALI': {},
        #'TISCALI_2': {}
    }

    # the warmup phase in which cache hits and misses are not counted
    WARMUP = 10000

    # Create experiments multiplexing all desired parameters
    for trace_name, N_REQUESTS, weights in traces:
        for strategy in STRATEGIES:
            for topology in TOPOLOGIES:
                param_names = TOPOLOGIES[topology].keys()
                no_params = param_names == []
                if not no_params:
                    topology_param_combinations = list(itertools.product(*[TOPOLOGIES[topology][param_name] for param_name in param_names]))
                else:
                    topology_param_combinations = [None]

                for topology_configuration_index, topology_configuration in enumerate(topology_param_combinations):
                    for cache_policy_index, cache_policy in enumerate(CACHE_POLICY):
                        experiment = Tree()
                        experiment['workload'] = {'name': 'DETERMINISTIC_TRACE_DRIVEN',
                                                  'n_warmup': WARMUP,
                                                  'n_measured': N_REQUESTS - WARMUP,
                                                  'reqs_file': 'resources/' + trace_name,
                                                  'weights': weights
                                                  }
                        experiment['cache_placement']['name'] = 'UNIFORM'
                        experiment['content_placement']['name'] = 'UNIFORM'
                        experiment['strategy']['name'] = strategy

                        experiment['topology']['name'] = topology
                        if not no_params:
                            for index, param_name in enumerate(param_names):
                                experiment['topology'][param_name] = topology_configuration[index]

                        experiment['cache_policy']['name'] = cache_policy
                        for param_name, param_value_list in CACHE_POLICY_PARAMETERS.items():
                            experiment['cache_policy'][param_name] = param_value_list[cache_policy_index]
                        experiment['cache_placement']['network_cache_per_node'] = NETWORK_CACHE_PER_NODE
                        experiment['cache_placement']['network_cache_all_nodes'] = NETWORK_CACHE_ALL_NODES
                        experiment['cache_placement']['network_cache_absolute'] = NETWORK_CACHE_ABSOLUTE
                        experiment[
                            'desc'] = "trace: %s, strategy: %s, cache policy: %s" \
                                      % (trace_name, strategy, cache_policy)

                        EXPERIMENT_QUEUE.append(experiment)
