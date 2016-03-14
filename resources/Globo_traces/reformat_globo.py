import getopt
import sys, os
from collections import defaultdict
from merge_globo import parse_line

def reformat(path, day, month, year, time_offset, contents, n_contents, file):
    day_existed = False
    for filename in os.listdir(path):
        if '%02d%02d%02d-merged.log' % (year, month, day) == filename:
            print 'reading %s' % filename
            day_existed = True
            with open(filename, 'rt') as in_file:

                for line in in_file:
                    request = parse_line(line)

                    if request['body_bytes_sent'] > 0 and request['http_request_name'] == 'GET' and \
                                    request['code'] < 400:

                        request_path = request['request_uri'].partition('?')[0][-4:]
                        if request_path == '.mp4':
                            timestamp = request['time'].partition('T')[2].partition('-')[0]
                            hours, _, timestamp = timestamp.partition(':')
                            minutes, _, seconds = timestamp.partition(':')
                            time = time_offset + int(hours) * 3600 + int(minutes) * 60 + int(seconds)

                            # content ID is after last slash and before next dash, format is after dash
                            content, _, format = request_path[::-1].partition('/')[2][::-1].partition('-')
                            if format not in contents:
                                contents.append(format)
                            else:
                                pass
                            #file.write('%d,%d,%s\n' % (time, 0, content))

    return day_existed, contents, n_contents


def main(argv):
    opts, args = getopt.getopt(argv, '', ['start-month=', 'end-month=', 'start-year=', 'end-year='])

    for opt, arg in opts:
        if opt == '--start-month':
            start_month = int(arg)
        elif opt == '--end-month':
            end_month = int(arg)
        elif opt == '--start-year':
            start_year = int(arg)
        elif opt == '--end-year':
            end_year = int(arg)

    if start_year > end_year:
        raise ValueError('start year has to be before or equal to end year')
    elif start_year == end_year:
        if start_month > end_month:
            raise ValueError('start month has to be before or equal to end month')

    n_contents = 0
    contents = {}
    time_offset = 0
    dmys = []

    for year in range(start_year, end_year + 1):
        if year == start_year and year == end_year:
            for month in range(start_month, end_month + 1):
                for day in range(1, 32):
                    dmys.append((day, month, year))
        elif year == start_year:
            for month in range(start_month, 13):
                for day in range(1, 32):
                    dmys.append((day, month, year))
        elif year == end_year:
            for month in range(1, end_month + 1):
                for day in range(1, 32):
                    dmys.append((day, month, year))
        else:
            for month in range(1, 13):
                for day in range(1, 32):
                    dmys.append((day, month, year))

    with open('%d%d-%d%d-reformatted.trace' %(start_month, start_year, end_month, end_year), 'wt') as reformatted_file:
        for dmy in dmys:
            day_existed, contents, n_contents = reformat('./', dmy[0], dmy[1], dmy[2], time_offset, contents, n_contents, reformatted_file)
            if day_existed:
                time_offset += 24 * 60 * 60 # seconds of one day
            print contents

if __name__ == "__main__":
    main(sys.argv[1:])