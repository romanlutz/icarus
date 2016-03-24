import getopt
import sys, os
from collections import defaultdict
from datetime import datetime, timedelta
from merge_globo import parse_line

def time_difference(timestamp1, timestamp2):
    year1, _, rest = timestamp1.partition('-')
    month1, _, rest = rest.partition('-')
    day1, _, rest = rest.partition('T')
    hour1, _, rest = rest.partition(':')
    min1, _, rest = rest.partition(':')
    sec1, _, rest = rest.partition('-')
    year2, _, rest = timestamp2.partition('-')
    month2, _, rest = rest.partition('-')
    day2, _, rest = rest.partition('T')
    hour2, _, rest = rest.partition(':')
    min2, _, rest = rest.partition(':')
    sec2, _, rest = rest.partition('-')

    [year1, year2, day1, day2, hour1, hour2, min1, min2, sec1, sec2] = \
        map(int, [year1, year2, day1, day2, hour1, hour2, min1, min2, sec1, sec2])

    # there are only January and Feburary requests, so this simple rule works
    month1 = 1 if month1 == 'Jan' else 2
    month2 = 1 if month2 == 'Jan' else 2

    time1 = datetime(year1, month1, day1, hour1, min1, sec1)
    time2 = datetime(year2, month2, day2, hour2, min2, sec2)

    if time2 > time1:
        delta = time2 - time1
    else:
        delta = time1 - time2

    if delta.days == 0:
        return delta.seconds
    else:
        return 86400


def create_default_data_dict():
    data = {}
    data['ip_24_ranges'] = defaultdict(int)
    data['ip_16_ranges'] = defaultdict(int)
    data['ip_8_ranges'] = defaultdict(int)
    data['request_names'] = defaultdict(int)
    data['body_bytes_sizes'] = defaultdict(int)
    data['content_type'] = defaultdict(int)
    data['content_type_hits'] = {}
    data['zero_bytes'] = 0
    #data['last_requests'] = {}

    return data

def clear_from_last_requests(timestamp, data):
    for ip in data['last_requests']:
        data['last_requests'][ip][:] = [request for request in data['last_requests'][ip] if time_difference(request['time'], timestamp) <= 10]

def print_data_dict_compact(data):
    print 'IP/24 ranges:', len(data['ip_24_ranges'])
    print 'IP/16 ranges:', len(data['ip_16_ranges'])
    print 'IP/8 ranges:', len(data['ip_8_ranges'])
    print 'request names:',
    for request_name in data['request_names']:
        print request_name, ':', data['request_names'][request_name], ',',
    print ''
    print 'body bytes sizes:',
    sizes = data['body_bytes_sizes'].keys()
    sizes.sort()
    for size in sizes:
        print size, ':', data['body_bytes_sizes'][size], ',',
    print ''
    for type in data['content_type']:
        print type, ':', data['content_type'][type], ',',
    print ''
    print data['content_type_hits']
    print 'requests with 0 bytes:', data['zero_bytes']

def analyze(path, day, month, year, data):
    timestamp = '%d-%d-%dT00:00:00-02:00' % (year, month, day)
    for filename in os.listdir(path):
        if '%02d%02d%02d-merged.log' % (year, month, day) == filename:
            print 'reading %s' % filename
            with open(filename, 'rt') as in_file:

                for line in in_file:
                    request = parse_line(line)

                    data['request_names'][request['http_request_name']] += 1

                    if request['http_request_name'] == 'GET' and request['code'] < 400:
                        data['zero_bytes'] += 1

                        ip_24 = ''.join(request['ip'][-1::-1].partition('.')[2])[-1::-1]
                        ip_16 = ''.join(ip_24[-1::-1].partition('.')[2])[-1::-1]
                        ip_8 = ''.join(ip_16[-1::-1].partition('.')[2])[-1::-1]

                        data['ip_24_ranges'][ip_24] += 1
                        data['ip_16_ranges'][ip_16] += 1
                        data['ip_8_ranges'][ip_8] += 1

                        byte_size = int(request['body_bytes_sent'])
                        order_of_magnitude = 1
                        while byte_size != 0:
                            byte_size = byte_size / 10
                            order_of_magnitude += 1
                        data['body_bytes_sizes']['10^%d < b < 10^%d' % (order_of_magnitude-1, order_of_magnitude)] += 1

                        format = None
                        if 'mp4' in request['request_uri'] and request['request_uri'].partition('?')[0][-4:] == 'm3u8':
                            format = '.mp4.m3u8'
                        elif request['request_uri'].partition('?')[0][-4:] == '.mp4':
                            format = '.mp4'
                        elif request['request_uri'].partition('?')[0][-3:] == '.ts':
                            format = '.ts'
                        elif request['request_uri'].partition('?')[0][-5:] == '.m3u8':
                            format = 'other .m3u8'
                        elif 'QualityLevels' in request['request_uri'].partition('.ism')[2] and \
                             'Fragments' in request['request_uri'].partition('.ism')[2]:
                            format = '.ism with QualityLevels and Fragments configuration'
                        elif '/manifest' == request['request_uri'].partition('.ism')[2].lower() or \
                                        request['request_uri'].partition('?')[0][-13:] == '.ism/manifest':
                            format = '.ism/manifest'
                        elif 'manifest' in request['request_uri'].partition('.ism')[2] and \
                             'Seg' in request['request_uri'].partition('.ism')[2] and \
                             'Frag' in request['request_uri'].partition('.ism')[2]:
                            format = '.ism with manifest and Seg/Frag index'
                        elif request['request_uri'].partition('.ism')[1] == '.ism' and \
                             '.f4m' in request['request_uri'].partition('.ism')[2]:
                            format = '.ism and .f4m'
                        elif request['request_uri'].partition('?')[0][-7:] == '.webvtt':
                            format = '.webvtt'
                        elif request['request_uri'].partition('?')[0][-4:] == '.mpd':
                            format = '.mpd'
                        elif request['request_uri'].partition('?')[0][-4:] == '.srt':
                            format = '.srt'
                        elif request['request_uri'].partition('?')[0][-8:] == '.drmfaxs':
                            format = '.drmfaxs'
                        elif request['request_uri'].partition('?')[0][-8:] == '.drmmeta':
                            format = 'drmmeta'
                        else:
                            print request

                        if format is not None:
                            data['content_type'][format] += 1
                            if request['cache_hit_or_miss'] == 'HIT':
                                data['content_type_hits'][format] = True

                    # clear old records from last-requests dictionary to save memory
                    # do this every 20 seconds because the last 10 seconds are saved
                    #if time_difference(timestamp, request['time']) > 20:
                    #    clear_from_last_requests(request['time'], data)

            print_data_dict_compact(data)

    return data

def main(argv):
    opts, args = getopt.getopt(argv, 'd:m:y:')

    for opt, arg in opts:
        if opt == '-d':
            day = int(arg)
        elif opt == '-m':
            month = int(arg)
        elif opt == '-y':
            year = int(arg)

    analyze('./', day, month, year, create_default_data_dict())

if __name__ == "__main__":
    main(sys.argv[1:])
