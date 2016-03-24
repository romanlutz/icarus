import getopt
import sys, os
from collections import defaultdict
from merge_globo import parse_line

def create_default_data_dict():
    data = {}
    data['ip_24_ranges'] = defaultdict(int)
    data['ip_16_ranges'] = defaultdict(int)
    data['ip_8_ranges'] = defaultdict(int)
    data['request_names'] = defaultdict(int)
    data['body_bytes_sizes'] = defaultdict(int)
    data['content_type'] = defaultdict(int)
    data['zero_bytes'] = 0

    return data

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
    print 'requests with 0 bytes:', data['zero_bytes']

def analyze(path, day, month, year, data):
    for filename in os.listdir(path):
        if '%02d%02d%02d-merged.log' % (year, month, day) == filename:
            print 'reading %s' % filename
            with open(filename, 'rt') as in_file:

                for line in in_file:
                    request = parse_line(line)

                    data['request_names'][request['http_request_name']] += 1

                    if request['body_bytes_sent'] > 0 and request['http_request_name'] == 'GET' and \
                                    request['code'] < 400:
                        data['zero_bytes'] += 1

                        ip_24 = ''.join(request['ip'][-1::-1].partition('.')[2])[-1::-1]
                        ip_16 = ''.join(ip_24[-1::-1].partition('.')[2])[-1::-1]
                        ip_8 = ''.join(ip_16[-1::-1].partition('.')[2])[-1::-1]

                        data['ip_24_ranges'][ip_24] += 1
                        data['ip_16_ranges'][ip_16] += 1
                        data['ip_8_ranges'][ip_8] += 1

                        data['body_bytes_sizes'][(int(request['body_bytes_sent']) / 1000000) * 1000000] += 1

                        if 'mp4' in request['request_uri'] and request['request_uri'].partition('?')[0][-4:] == 'm3u8':
                            data['content_type']['.mp4.m3u8'] += 1
                        elif request['request_uri'].partition('?')[0][-4:] == '.mp4':
                            data['content_type']['.mp4'] += 1
                        elif request['request_uri'].partition('?')[0][-3:] == '.ts':
                            data['content_type']['.ts'] += 1
                        elif request['request_uri'].partition('?')[0][-5:] == '.m3u8':
                            data['content_type']['other .m3u8'] +=1
                        elif 'QualityLevels' in request['request_uri'].partition('.ism')[2] and \
                             'Fragments' in request['request_uri'].partition('.ism')[2]:
                            data['content_type']['.ism with QualityLevels and Fragments configuration'] += 1
                        elif '/manifest' == request['request_uri'].partition('.ism')[2].lower() or \
                                        request['request_uri'].partition('?')[0][-13:] == '.ism/manifest':
                            data['content_type']['.ism/manifest'] += 1
                        elif 'manifest' in request['request_uri'].partition('.ism')[2] and \
                             'Seg' in request['request_uri'].partition('.ism')[2] and \
                             'Frag' in request['request_uri'].partition('.ism')[2]:
                             data['content_type']['.ism with manifest and Seg/Frag index'] += 1
                        elif request['request_uri'].partition('.ism')[1] == '.ism' and \
                             '.f4m' in request['request_uri'].partition('.ism')[2]:
                             data['content_type']['.ism and .f4m'] += 1
                        elif request['request_uri'].partition('?')[0][-7:] == '.webvtt':
                            data['content_type']['.webvtt'] += 1
                        elif request['request_uri'].partition('?')[0][-4:] == '.mpd':
                            data['content_type']['.mpd'] += 1
                        elif request['request_uri'].partition('?')[0][-4:] == '.srt':
                            data['content_type']['.srt'] += 1
                        elif request['request_uri'].partition('?')[0][-8:] == '.drmfaxs':
                            data['content_type']['.drmfaxs'] += 1
                        elif request['request_uri'].partition('?')[0][-8:] == '.drmmeta':
                            data['content_type']['.drmmeta'] += 1
                        else:
                            print request

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
