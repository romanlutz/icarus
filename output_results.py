__author__ = 'romanlutz'
from icarus.results.readwrite import read_results_pickle

def main():
    result = read_results_pickle('results.pickle')
    for tree in result:
        for k in tree[0]:
            print k
        for k in tree[1]:
            print k


main()
