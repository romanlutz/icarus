import getopt
import sys, os
from collections import defaultdict
from merge_globo import parse_line


def analyze(path, day, month, year):
    for filename in os.listdir(path):
        if '%02d%02d%02d-merged.log' % (year, month, day) == filename:
            print 'reading %s' % filename
            with open(filename, 'rt') as in_file:
                ip_24_ranges = defaultdict(int)
                ip_16_ranges = defaultdict(int)
                ip_8_ranges = defaultdict(int)

                http_codes = defaultdict(int)

                request_names = defaultdict(int)

                body_bytes_sizes = defaultdict(int)

                content_version = defaultdict(int)

                zero_bytes = 0

                for line in in_file:
                    request = parse_line(line)

                    ip_24 = ''.join(request['ip'][-1::-1].partition('.')[2])[-1::-1]
                    ip_16 = ''.join(ip_24[-1::-1].partition('.')[2])[-1::-1]
                    ip_8 = ''.join(ip_16[-1::-1].partition('.')[2])[-1::-1]

                    ip_24_ranges[ip_24] += 1
                    ip_16_ranges[ip_16] += 1
                    ip_8_ranges[ip_8] += 1

                    http_codes[request['code']] += 1

                    request_names[request['http_request_name']] += 1

                    if 'mp4' in request['request_uri'] and request['request_uri'].partition('?')[0][-4:] == 'm3u8':
                        content_version['mp4'] += 1

                        body_bytes_sizes[(int(request['body_bytes_sent']) / 10000) * 1000000] += 1

                        if request['body_bytes_sent'] == 0:
                            zero_bytes += 1

            print 'IPs:', len(ip_8_ranges), len(ip_16_ranges), len(ip_24_ranges)
            print http_codes
            print request_names
            buckets = body_bytes_sizes.keys()
            buckets.sort()
            for bucket in buckets:
                print bucket, ':', body_bytes_sizes[bucket], '; ',
            print '\n'
            print 'requests with 0 bytes:', zero_bytes
            print content_version

def main(argv):
    opts, args = getopt.getopt(argv, 'd:m:y:')

    for opt, arg in opts:
        if opt == '-d':
            day = int(arg)
        elif opt == '-m':
            month = int(arg)
        elif opt == '-y':
            year = int(arg)

    analyze('./', day, month, year)

if __name__ == "__main__":
    main(sys.argv[1:])
