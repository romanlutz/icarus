"""Functions for reading and writing results
"""
import collections
import copy
try:
    import pickle as pickle
except ImportError:
    import pickle
from icarus.util import Tree
from icarus.registry import register_results_reader, register_results_writer
from . import spickle

__all__ = [
    'ResultSet',
    'write_results_pickle',
    'read_results_pickle'
           ]

class ResultSet(object):
    """This class can be used to store results from different experiments,
    accessed and filtered.
    
    A result set is basically a list of results, one per each experiment. Each
    entry of the resultset is a 2-tuple referring to a single experiment.
    In this 2-tuple: 
     * the first element is a tree with all parameters of the experiment
     * the second element is a tree with all results of the experiment 
    
    All operations that write data are thread-safe so that this object can 
    be shared by different processes.
    """
    
    def __init__(self, attr=None):
        """Constructor
        
        Parameters
        ----------
        attr : dict, optional
            Dictionary of common attributes to all experiments
        """
        self._results = collections.deque()
        # Dict of global attributes common to all experiments
        self.attr = attr if attr is not None else {}
    
    def __len__(self):
        """Returns the number of results in the resultset
        
        Returns
        -------
        len : int
            The length of the resultset
        """
        return len(self._results)
    
    def __iter__(self):
        """Returns iterator over the resultset
        
        Returns
        -------
        iter : iterator
            Iterator over the resultset
        """
        return iter(self._results)
        
    def __getitem__(self, i):
        """Returns a specified item of the resultset
        
        Parameters
        ----------
        i : int
            The index of the result
            
        Returns
        -------
        result : tuple
            Result
        """
        return self._results[i]

    def __add__(self, resultset):
        """Merges two resultsets.
        
        Parameters
        ----------
        resultset : ResultSet
            The result set to merge
        
        Returns
        -------
        resultset : ResultSet
            The resultset containing results from this resultset and the one
            passed as argument
        """
        if self.attr != resultset.attr:
            raise ValueError('The resultsets cannot be merged because '
                             'they have different global attributes')
        rs = copy.deepcopy(self)
        for i in iter(resultset):
            rs.add(*i)
        return rs

    def add(self, parameters, results):
        """Add a result to the result set.
        
        Parameters
        ----------
        parameters : Tree
            Tree of experiment parameters
        results : Tree
            Tree of experiment results
            
        Notes
        -----
        If parameters and results are dictionaries, this method will attempt to
        convert them to trees and storing them anyway. It is necessary that
        parameters and results are saved as trees so that plotting functions
        can search correctly in them.
        """
        if not isinstance(parameters, Tree):
            parameters = Tree(parameters)
        if not isinstance(results, Tree):
            results = Tree(results)
        self._results.append((parameters, results))
    
    def dump(self):
        """Dump all results.
        
        Returns
        -------
        results : list
            A list of 2-value tuples where the first value is the dictionary
            of experiment parameters and the second value is the dictionary
            of experiment results.
        """
        return list(self._results)

    
    def filter(self, condition):
        """Return subset of results matching specific conditions
        
        Parameters
        ----------
        condition : dict
            Dictionary listing all parameters and values to be matched in the
            results set. Each parameter, i.e., each key of the dictionary must
            be an iterable object containing the path in the parameters tree
            to the required parameter 
        metrics : dict, optional
            List of metrics to be reported
        
        Returns
        -------
        filtered_results : ResultSet
            List of 2-tuples of filtered results, where the first element is a
            tree of all experiment parameters and the second value is 
            a tree with experiment results.
        """
        filtered_resultset = ResultSet()
        for parameters, results in self._results:
            parameters = Tree(parameters)
            if parameters.match(condition):
                filtered_resultset.add(parameters, results)
        return filtered_resultset


@register_results_writer('PICKLE')
def write_results_pickle(results, path):
    """Write a resultset to a pickle file
    
    Parameters
    ----------
    results : ResultSet
        The set of results
    path : str
        The path of the file to which write
    """
    with open(path, 'wb') as pickle_file:
        pickle.dump(results, pickle_file)


@register_results_reader('PICKLE')
def read_results_pickle(path):
    """Reads a resultset from a pickle file.
    
    Parameters
    ----------
    path : str
        The file path from which results are read
    
    Returns
    -------
    results : ResultSet
        The read result set
    """
    with open(path, 'rb') as pickle_file:
        return pickle.load(pickle_file)


@register_results_writer('SPICKLE')
def write_results_spickle(results, path):
    """Write a resultset to a streaming pickle file

    Parameters
    ----------
    results : ResultSet
        The set of results
    path : str
        The path of the file to which write
    """
    with open(path, 'wb') as spickle_file:
        spickle.s_dump(results, spickle_file)


@register_results_reader('SPICKLE')
def read_results_spickle(path):
    """Reads a resultset from a streaming pickle file. This allows for iterative file reading instead of loading the
    whole file first.

    Parameters
    ----------
    path : str
        The file path from which results are read

    Returns
    -------
    results : ResultSet
        The read result set
    """
    return spickle.s_load(open(path, 'rb'))

def read_results(path, format):
    if format == '.pickle':
        return read_results_pickle(path)
    elif format == '.spickle':
        return read_results_spickle(path)
    else:
        raise ValueError('format has to be either .pickle or .spickle')


