import getopt
import sys, os
from collections import defaultdict
from .merge_globo import parse_line
from .analyze_globo import mp4_versions, determine_format_and_content_id, clear_from_last_requests

priority_content_types = ['fantastico', 'jornal-nacional', 'bom-dia-brasil', 'globo-news', 'jornal-da-globo', 'sportv']


def reformat(path, day, month, year, time_offset, contents, n_contents, out_file):
    day_existed = False
    for filename in os.listdir(path):
        if '%02d%02d%02d-merged.log' % (year, month, day) == filename:
            print(('reading %s' % filename))
            day_existed = True
            with open(filename, 'rt') as in_file:

                for line in in_file:
                    request = parse_line(line)

                    if request['http_request_name'] == 'GET' and request['code'] < 300:

                        try:
                            _, content_id = determine_format_and_content_id(request)
                        except:
                            content_id = None

                        # take content IDs for generating the trace only if they have been found (not None)
                        if content_id is not None:
                            time = request['time'].rpartition('T')[2].rpartition('-')[0]
                            hours, _, time = time.partition(':')
                            minutes, _, seconds = time.partition(':')
                            time = time_offset + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                            # currently the receiver is constant (one cache scenario), set to 0
                            out_file.write('%d,%d,%d\n' % (time, 0, content_id))

                            # determine weight of content
                            if content_id not in contents or contents[content_id] == 1:
                                is_priority_content = False
                                for priority_content_type in priority_content_types:
                                    if priority_content_type in line:
                                        is_priority_content = True
                                        break

                                weight = 2 if is_priority_content else 1
                                contents[content_id] = weight



                    # clear old records from last-requests dictionary to save memory
                    # do this every 20 seconds because the last 10 seconds are saved
                    #if time_difference(timestamp, request['time']) > 20:
                    #    clear_from_last_requests(request['time'], data)

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
            day_existed, contents, n_contents = reformat('./', dmy[0], dmy[1], dmy[2], time_offset, contents,
                                                         n_contents, reformatted_file)
            if day_existed:
                time_offset += 24 * 60 * 60 # seconds of one day
    with open('%d%d-%d%d-reformatted.weights' % (start_month, start_year, end_month, end_year), 'wt') as weights_file:
        for content_id in contents:
            weights_file.write('%d,%d\n' % (content_id, contents[content_id]))

if __name__ == "__main__":
    main(sys.argv[1:])