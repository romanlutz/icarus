import getopt
import sys

from merge_globo import merge


def main(argv):
    opts, args = getopt.getopt(argv, '', ['start-day=', 'end-day=', 'month=', 'year='])

    for opt, arg in opts:
        if opt == '--start-day':
            start_day = int(arg)
        elif opt == '--end-day':
            end_day = int(arg)
        elif opt == '--month':
            month = int(arg)
        elif opt == '--year':
            year = int(arg)

    if start_day > end_day:
        raise ValueError('start day has to be before or equal to end day')

    for day in range(start_day, end_day + 1):
        merge('./', day, month, year)

if __name__ == "__main__":
    main(sys.argv[1:])
