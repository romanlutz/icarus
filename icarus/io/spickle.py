"""
This file was copied from https://gist.github.com/miku/4268445

Streaming pickle implementation for efficiently serializing and
de-serializing an iterable (e.g., list)
Originally created on 2010-06-19 by Philip Guo
Reformatted on 2012-12-12 by M Czygan
Limitations:
  - Only works with ASCII-based pickles (protocol=0), since s_load reads
  in one line at a time
"""


try:
    from pickle import dumps, loads
except ImportError:
    from pickle import dumps, loads


def s_dump(iterable_to_pickle, file_obj):
    """ dump contents of an iterable iterable_to_pickle to file_obj, a file
    opened in write mode """
    for elt in iterable_to_pickle:
        s_dump_elt(elt, file_obj)

def s_dump_elt(elt_to_pickle, file_obj):
    """ dumps one element to file_obj, a file opened in write mode """
    pickled_elt_str = dumps(elt_to_pickle)
    file_obj.write(pickled_elt_str)
    # record separator is a blank line
    # (since pickled_elt_str might contain its own newlines)
    file_obj.write(b'\n\n')

def s_load(file_obj):
    """ load contents from file_obj, returning a generator that yields one
        element at a time """
    cur_elt = []
    for line in file_obj:
        cur_elt.append(line)

        if line == b'\n':
            pickled_elt_str = ''.join(cur_elt)
            elt = loads(pickled_elt_str)
            cur_elt = []
            yield elt