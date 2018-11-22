import csv
import sys
import getopt
import random

def main(argv):
    opts, args = getopt.getopt(argv, 'w:p:t:n:r:')

    for opt, arg in opts:
        if opt == '-w':
            weights = list(map(int, arg.split(',')))
        elif opt == '-p':
            percentages = list(map(float, arg.split(',')))
        elif opt == '-t':
            trace_file = arg
        elif opt == '-n':
            new_file_prefix = arg
        elif opt == '-r':
            repetitions = int(arg)

    contents = {}
    with open(trace_file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = int(row[2])
            contents[content] = 1

    number_of_contents = len(contents)
    keys = list(contents.keys())

    for repetition in range(repetitions):
        random.shuffle(keys)

        for percentage in percentages:
            weighted_contents = keys[:int(percentage * number_of_contents)]

            for weight in weights:
                for key in weighted_contents:
                    contents[key] = weight

                with open('%s-w%d-p%f-r%d.weights' % (new_file_prefix, weight, percentage, repetition), 'wt') as weights_file:
                    for content in contents:
                        weights_file.write('%d,%d\n' % (content, contents[content]))

                # undo weighting
                for key in weighted_contents:
                    contents[key] = 1

if __name__ == "__main__":
    main(sys.argv[1:])