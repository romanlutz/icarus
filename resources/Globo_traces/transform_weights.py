import csv
import sys
import getopt
from collections import defaultdict

def main(argv):
    opts, args = getopt.getopt(argv, 'o:n:')

    for opt, arg in opts:
        if opt == '-o':
            change_weight = int(arg)
        elif opt == '-n':
            new_weight = int(arg)

    weights = {}
    with open('12-2015-01-2016.weights', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = int(row[0])
            weight = int(row[1])

            weights[content] = weight

    for content in weights:
        if weights[content] == change_weight:
            weights[content] = new_weight

    with open('12-2015-01-2016.weights', 'wt') as weights_file:
        for content in weights:
            weights_file.write('%d,%d\n' % (content, weights[content]))

if __name__ == "__main__":
    main(sys.argv[1:])