

import sys
if sys.version_info[:2] < (2, 6):
    m = "Python version 2.6 or later is required for Icarus (%d.%d detected)."
    raise ImportError(m % sys.version_info[:2])
del sys

# Author information
__author__ = 'Lorenzo Saino, Ioannis Psaras'

# Version information
__version__ = '0.5.0'

# License information
___license___ = 'GNU GPLv2'

# List of all modules (even outside Icarus) that contain classes or function
# needed to be registered with the registry (via a register decorator)
# This code ensures that the modules are imported and hence the decorators are
# executed and the classes/functions registered. 
__modules_to_register = [
     'icarus.models.cache',
     'icarus.models.space_saving',
     'icarus.models.data_stream_caching_algorithm',
     'icarus.models.adaptive_replacement_cache',
     'icarus.models.k_lru',
     'icarus.models.strategy',
     'icarus.execution.collectors', 
     'icarus.io.readwrite',
     'icarus.scenarios.topology',
     'icarus.scenarios.contentplacement',
     'icarus.scenarios.cacheplacement',
     'icarus.scenarios.workload',
     'icarus.tools.traces',
     'icarus.results.visualize',
                         ]

for m in __modules_to_register:
    # This try/catch is needed to support reload(icarus)
    try:
        exec('import %s' % m)
        exec('del %s' % m) 
    except AttributeError:
        pass
del m

# Imports
from .models import *
from .tools import *
