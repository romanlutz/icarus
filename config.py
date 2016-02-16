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

Experiment definition syntax
============================

This figure below represents the parameter structure accepted by Icarus:

 |
 |--- topology
 |        |----- name
 |        |----- topology arg 1
 |        |----- topology arg 2
 |        |----- ..............
 |        |----- topology arg N
 |
 |--- workload
 |        |----- name
 |        |----- workload arg 1
 |        |----- workload arg 2
 |        |----- ..............
 |        |----- workload arg N
 |
 |--- cache_placement
 |        |----- name
 |        |----- cache_placement arg 1
 |        |----- cache_placement arg 2
 |        |----- ......................
 |        |----- cache_placement arg N
 |
 |--- content_placement
 |        |----- name
 |        |----- content_placement arg 1
 |        |----- content_placement arg 2
 |        |----- .......................
 |        |----- content_placement arg N
 |
 |--- strategy
 |        |----- name
 |        |----- strategy arg 1
 |        |----- strategy arg 2
 |        |----- ..............
 |        |----- strategy arg N
 |
 |--- cache_policy
 |        |----- name
 |        |----- cache_policy arg 1
 |        |----- cache_policy arg 2
 |        |----- ..................
 |        |----- cache_policy arg N
 |


Here below are listed all components currently provided by Icarus and lists
all parameters for each of them

topology
--------

Path topology
 * name: PATH
 * args:
    * n: number of nodes

Tree topology
 * name: TREE
 * args:
    * h: height
    * k: branching factor
    
RocketFuel topologies
 * name: ROCKET_FUEL
 * args:
     * asn: ASN of topology selected (see resources/README.md for further info)
     * source_ratio: ratio of nodes to which attach a content source
     * ext_delay: delay of interdomain links

Internet Topology Zoo topologies 
 * name: GARR, GEANT, TISCALI, WIDE, GEANT_2, GARR_2, TISCALI_2
 * args: None


workload
--------

Stationary Zipf workload
 * name: STATIONARY
 * args:
    * alpha : float, the Zipf alpha parameter
    * n_contents: number of content objects
    * n_warmup: number of warmup requests
    * n_measured: number of measured requests
    * rate: requests rate

GlobeTraff workload
 * name: GLOBETRAFF
 * args:
    * reqs_file: the path to a GlobeTraff request file
    * contents_file: the path to a GlobeTraff content file

Trace-driven workload
 * name: TRACE_DRIVEN
 * args:
    * reqs_file: the path to the requests file
    * contents_file: the path to the contents file
    * n_contents: number of content objects
    * n_warmup: number of warmup requests
    * n_measured: number of measured requests


content_placement
-----------------
Uniform (content uniformly distributed among servers)
 * name: UNIFORM 
 * args: None 


cache_placement
---------------
 * name:
    * UNIFORM -> cache space uniformly spread across caches
    * CONSOLIDATED -> cache space consolidated among nodes with top betweenness centrality
    * BETWEENNESS_CENTRALITY -> cache space assigned to all candidate nodes proportionally to their betweenness centrality
    * DEGREE -> cache space assigned to all candidate nodes proportionally to their degree
 * args
    * For all:
       * network_cache: overall network cache (in number of entries) as fraction of content catalogue 
    * For CONSOLIDATED
       * spread: The fraction of top centrality nodes on which caches are deployed (optional, default: 0.5)


strategy
--------
 * name:
    * LCE             ->  Leave Copy Everywhere
    * NO_CACHE        ->  No caching, shorest-path routing
    * HR_SYMM         ->  Symmetric hash-routing
    * HR_ASYMM        ->  Asymmetric hash-routing
    * HR_MULTICAST    ->  Multicast hash-routing
    * HR_HYBRID_AM    ->  Hybrid Asymm-Multicast hash-routing
    * HR_HYBRID_SM    ->  Hybrid Symm-Multicast hash-routing
    * CL4M            ->  Cache less for more
    * PROB_CACHE      ->  ProbCache
    * LCD             ->  Leave Copy Down
    * RAND_CHOICE     ->  Random choice: cache in one random cache on path
    * RAND_BERNOULLI  ->  Random Bernoulli: cache randomly in caches on path
 * args:
    * For PROB_CACHE
       * t_tw : float, optional, default=10. The ProbCache t_tw parameter
    * For HR_HYBRID_AM
       * max_stretch: float, optional, default=0.2.
         The max detour stretch for selecting multicast 


cache_policy
------------
 * name:
    * LRU   -> Least Recently Used
    * SLRU  -> Segmeted Least Recently Used
    * LFU   -> Least Frequently Used
    * NULL  -> No cache
    * RAND  -> Random eviction
    * FIFO  -> First In First Out
 * args:
    * For SLRU:
       * segments: int, optional, default=2. Number of segments 


desc
----
string describing the experiment (used to print on screen progress information)

Further info
============

To get further information about the models implemented in the simulator you
can inspect the source code which is well organized and documented:
 * Topology implementations are located in ./icarus/scenarios/topology.py
 * Cache placement implementations are located in
   ./icarus/scenarios/cacheplacement.py
 * Caching and routing strategies located in ./icarus/models/strategy.py
 * Cache eviction policy implementations are located in ./icarus/models/cache.py
"""
from multiprocessing import cpu_count
from collections import deque
import copy, csv
from icarus.util import Tree

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

# Number of times each experiment is replicated
# This is necessary for extracting confidence interval of selected metrics
N_REPLICATIONS = 1

# List of metrics to be measured in the experiments
# The implementation of data collectors are located in ./icarus/execution/collectors.py
# Remove collectors not needed
DATA_COLLECTORS = [
           'CACHE_HIT_RATIO',   # Measure cache hit ratio 
           'LATENCY',           # Measure request and response latency (based on static link delays)
           #'LINK_LOAD',         # Measure link loads
           'PATH_STRETCH',      # Measure path stretch
           'CACHE_LEVEL_PROPORTIONS',
           'WINDOW_SIZE']



########################## EXPERIMENTS CONFIGURATION ##########################

# Default experiment values, i.e. values shared by all experiments

# Number of requests per second (over the whole network)
REQ_RATE = 1.0

# Total size of network cache as a fraction of content population
# If the cache size is a static number (e.g. 100), set NETWORK_CACHE_FRACTION to False
# In case the cache size is given as a natural number, set it to the cumulative total of the whole network
NETWORK_CACHE = 1000
NETWORK_CACHE_FRACTION = False

# if running a trace-driven simulation, REQ_FILE is the path to the trace file
traces = []
with open('resources/trace_overview.csv', 'r') as trace_file:
    csv_reader = csv.reader(trace_file)
    i = 1
    for line in csv_reader:
        if i >= 29 or i <= 5:
            traces.append((line[0], int(line[1])))
        i += 1


def append_default(cache_policy_parameters, window_size=False, subwindows=False, subwindow_size=False, monitored=False, warmup=False,
                   segments=False, cached_segments=False, lru_portion=False, hypothesis_check_period=False,
                   hypothesis_check_A=False, hypothesis_check_epsilon=False):
    variables = ['window_size', 'subwindows', 'subwindow_size', 'monitored', 'segments', 'cached_segments', 'lru_portion',
                 'hypothesis_check_period', 'hypothesis_check_A', 'hypothesis_check_epsilon']
    for variable in variables:
        if eval(variable):
            cache_policy_parameters[variable].append(None)
    if warmup:
        cache_policy_parameters['warmup'].append(NETWORK_CACHE*4)

# Cache eviction policy
CACHE_POLICY = []
CACHE_POLICY_PARAMETERS = {'window_size': [], 'subwindows': [], 'subwindow_size': [], 'monitored': [], 'warmup': [],
                           'segments': [], 'cached_segments': [], 'lru_portion': [], 'hypothesis_check_period': [],
                           'hypothesis_check_A': [], 'hypothesis_check_epsilon': []}



MONITORED_DEFAULT = NETWORK_CACHE * 2
use_SS = False
use_DSCA = True
use_DSCAAWS = True
use_DSCASW = True
use_DSCAFT = True
use_DSCAFS = True
use_ADSCASTK = True
use_ADSCAATK = True
use_ARC = False
use_LRU = False
use_KLRU = False

if use_SS:
    CACHE_POLICY.append('SS')
    CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
    append_default(CACHE_POLICY_PARAMETERS, window_size=True, subwindows=True, subwindow_size=True,
                   segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True, warmup=True)

if use_DSCA:
    for window_size in [MONITORED_DEFAULT*4, MONITORED_DEFAULT*16, MONITORED_DEFAULT*64]:
        CACHE_POLICY.append('DSCA')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_DSCAAWS:
    for hypothesis_check_period in [1, 500, 1000, 2000, 4000]:
        for hypothesis_check_A in [0.33]:
            for hypothesis_check_epsilon in [0.005]:
                CACHE_POLICY.append('DSCAAWS')
                CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
                CACHE_POLICY_PARAMETERS['hypothesis_check_period'].append(hypothesis_check_period)
                CACHE_POLICY_PARAMETERS['hypothesis_check_A'].append(hypothesis_check_A)
                CACHE_POLICY_PARAMETERS['hypothesis_check_epsilon'].append(hypothesis_check_epsilon)
                append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                               cached_segments=True, lru_portion=True, window_size=True)

if use_DSCASW:
    for subwindow_size in [MONITORED_DEFAULT, MONITORED_DEFAULT*4, MONITORED_DEFAULT*16]:
        for subwindows in range(2, 11, 2):
            CACHE_POLICY.append('DSCASW')
            CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
            CACHE_POLICY_PARAMETERS['subwindows'].append(subwindows)
            CACHE_POLICY_PARAMETERS['subwindow_size'].append(subwindow_size)
            append_default(CACHE_POLICY_PARAMETERS, window_size=True, warmup=True, segments=True, cached_segments=True,
                           lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)

if use_DSCAFT:
    for window_size in [MONITORED_DEFAULT*4, MONITORED_DEFAULT*16, MONITORED_DEFAULT*64]:
        CACHE_POLICY.append('DSCAFT')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        # the threshold is used as specified through the default value
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_DSCAFS:
    for window_size in [MONITORED_DEFAULT, MONITORED_DEFAULT*4, MONITORED_DEFAULT*16]:
        for lru_portion in [0.1, 0.25, 0.5, 0.75, 0.9]:
            CACHE_POLICY.append('DSCAFS')
            CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
            CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
            CACHE_POLICY_PARAMETERS['lru_portion'].append(lru_portion)
            append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                           cached_segments=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)

if use_ADSCASTK:
    for window_size in [MONITORED_DEFAULT, MONITORED_DEFAULT*4, MONITORED_DEFAULT*16, MONITORED_DEFAULT*64]:
        CACHE_POLICY.append('ADSCASTK')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_ADSCAATK:
    for window_size in [MONITORED_DEFAULT, MONITORED_DEFAULT*4, MONITORED_DEFAULT*16, MONITORED_DEFAULT*64]:
        CACHE_POLICY.append('ADSCAATK')
        CACHE_POLICY_PARAMETERS['monitored'].append(MONITORED_DEFAULT)
        CACHE_POLICY_PARAMETERS['window_size'].append(window_size)
        append_default(CACHE_POLICY_PARAMETERS, subwindows=True, subwindow_size=True, warmup=True, segments=True,
                       cached_segments=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                       hypothesis_check_epsilon=True)

if use_ARC:
    CACHE_POLICY.append('ARC')
    append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True, subwindow_size=True,
                   warmup=True, segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True)

if use_LRU:
    CACHE_POLICY.append('LRU')
    append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True, subwindow_size=True,
                   warmup=True, segments=True, cached_segments=True, lru_portion=True, hypothesis_check_period=True,
                   hypothesis_check_A=True, hypothesis_check_epsilon=True)

if use_KLRU:
    for segments in [2, 3]:
        for cached_segments in range(1, segments):
            CACHE_POLICY.append('KLRU')
            CACHE_POLICY_PARAMETERS['cached_segments'].append(cached_segments)
            CACHE_POLICY_PARAMETERS['segments'].append(segments)
            append_default(CACHE_POLICY_PARAMETERS, monitored=True, window_size=True, subwindows=True, subwindow_size=True,
                           warmup=True, lru_portion=True, hypothesis_check_period=True, hypothesis_check_A=True,
                           hypothesis_check_epsilon=True)


# Zipf alpha parameter for non-trace-driven simulation
ALPHA = [0.8]#[0.6, 0.8, 1.0]

# List of topologies tested
# Topology implementations are located in ./icarus/scenarios/topology.py
# Remove topologies not needed
TOPOLOGIES =  [
        'PATH'
        #'GEANT',
        #'WIDE',
        #'GARR',
        #'TISCALI',
              ]

TOPOLOGY_PARAMS = {'PATH': {'n': 3, 'delay': 0}}

# List of caching and routing strategies
# The code is located in ./icarus/models/strategy.py
# Remove strategies not needed
STRATEGIES = [
     'LCE',             # Leave Copy Everywhere
     #'NO_CACHE',        # No caching, shortest-path routing
     #'HR_SYMM',         # Symmetric hash-routing
     #'HR_ASYMM',        # Asymmetric hash-routing
     #'HR_MULTICAST',    # Multicast hash-routing
     #'HR_HYBRID_AM',    # Hybrid Asymm-Multicast hash-routing
     #'HR_HYBRID_SM',    # Hybrid Symm-Multicast hash-routing
     #'CL4M',            # Cache less for more
     #'PROB_CACHE',      # ProbCache
     #'LCD',             # Leave Copy Down
     #'RAND_CHOICE',     # Random choice: cache in one random cache on path
     #'RAND_BERNOULLI',  # Random Bernoulli: cache randomly in caches on path
             ]

# Instantiate experiment queue
EXPERIMENT_QUEUE = deque()

# Build a default experiment configuration which is going to be used by all
# experiments of the campaign


# Create experiments multiplexing all desired parameters
for trace_name, N_CONTENTS in traces:
    for alpha in ALPHA:
        for strategy in STRATEGIES:
            for topology in TOPOLOGIES:
                for cache_policy_index, cache_policy in enumerate(CACHE_POLICY):
                    experiment = Tree()
                    experiment['workload'] = {'name':    'DETERMINISTIC_TRACE_DRIVEN',
                                           'n_contents': N_CONTENTS,
                                           'n_warmup':   CACHE_POLICY_PARAMETERS['warmup'][cache_policy_index],
                                           'n_measured': N_CONTENTS - CACHE_POLICY_PARAMETERS['warmup'][cache_policy_index],
                                           'rate':       REQ_RATE,
                                           'reqs_file':  'resources/' + trace_name
                                          }
                    experiment['cache_placement']['name'] = 'UNIFORM'
                    experiment['content_placement']['name'] = 'UNIFORM'
                    experiment['workload']['alpha'] = alpha
                    experiment['strategy']['name'] = strategy

                    experiment['topology']['name'] = topology
                    if topology in TOPOLOGY_PARAMS.keys():
                        for topology_param in TOPOLOGY_PARAMS[topology].keys():
                            experiment['topology'][topology_param] = TOPOLOGY_PARAMS[topology][topology_param]

                    experiment['cache_policy']['name'] = cache_policy
                    for param_name, param_value_list in CACHE_POLICY_PARAMETERS.items():
                        if param_name != 'warmup':
                            experiment['cache_policy'][param_name] = param_value_list[cache_policy_index]
                    experiment['cache_placement']['network_cache'] = NETWORK_CACHE
                    experiment['cache_placement']['network_cache_fraction'] = NETWORK_CACHE_FRACTION
                    experiment['desc'] = "strategy: %s, topology: %s, network cache: %s, cache policy: %s, trace: %s" \
                                         % (strategy, topology, str(NETWORK_CACHE), cache_policy, trace_name)

                    EXPERIMENT_QUEUE.append(experiment)


