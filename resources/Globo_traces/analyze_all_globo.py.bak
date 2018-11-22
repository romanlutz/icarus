import getopt
import sys

from analyze_globo import analyze, create_default_data_dict


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

    data = create_default_data_dict()

    for day in range(start_day, end_day + 1):
        data = analyze('./', day, month, year, data)

if __name__ == "__main__":
    main(sys.argv[1:])
