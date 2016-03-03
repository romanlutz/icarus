from icarus.io.compression import unzip_file
import os
from collections import defaultdict
from shutil import copyfile
import fileinput


def parse_line(line):
    request = {}
    request['time'], _, rest = line.partition(' ')
    request['ip'], _, rest = rest.partition(' (')
    request['code'], _, rest = rest.partition(') ')
    request['cache_hit_or_miss'], _, rest= rest.partition(' ')
    request['body_bytes_sent'], _, rest = rest.partition(' ')
    request['answer_time'], _, rest = rest.partition(' [')
    request['upstream_response_time'], _, rest = rest.partition('] [\"') # only relevant in case of cache miss
    request['http_request_name'], _, rest = rest.partition(' ')
    request['http_or_https'], _, rest = rest.partition(' ')
    request['host'], _, rest = rest.partition(' ')
    request['request_uri'], _, rest = rest.partition('\"] [\"')
    request['referrer'], _, rest = rest.partition('\"] \"[')
    request['user_agent'], _, rest = rest.partition('\"] [')
    request['forwarded_for'], _, _ = rest.partition(']')
    return request

def merge(path, day, month, year):
    trace_files = []
    for filename in os.listdir(path):
        if '%02d%02d%02d' % (year, month, day) in filename and filename[-4:] == '.log':
            trace_files.append(filename)

    if trace_files == []:
        return

    # select first file as initial merge file, unzip it and rename it
    unzip_file(path + trace_files[0])
    merged_filename = '%02d%02d%02d-merged.log' % (year, month, day)
    os.rename(path + trace_files[0][:-3], merged_filename)

    # for the rest merge them one by one with the merged file
    # the files are assumed to have ordered records
    temporary_merged_filename = '%02d%02d%02d-temp-merged.log' % (year, month, day)


    for filename in trace_files[1:]:
        print 'unzipping %s' % filename
        # unzip file while keeping original
        unzip_file(path + filename)

        try:
            input_filename = filename[:-3]
            print 'reading %s' % input_filename

            in_file = fileinput.input(input_filename)
            merged_file = fileinput.input(merged_filename)

            with open(temporary_merged_filename, 'wt') as temporary_merged_file:

                input_request = parse_line(in_file.next())
                merged_request = parse_line(merged_file.next())

                while True:
                    if input_request['time'] < merged_request['time']:
                        temporary_merged_filename.write(encode_dict(input_request))
                        input_request = parse_line(in_file.next())
                    else:
                        temporary_merged_filename.write(encode_dict(merged_request))
                        merged_request = parse_line(merged_file.next())

        finally:
            print 'deleting %s' % input_filename
            # delete unzipped file - original still exists
            os.remove(path + input_filename)

def analyze(path, day, month, year):
    for filename in os.listdir(path):
        if '%02d%02d%02d' % (year, month, day) in filename and filename[-3:]:
            print 'unzipping %s' % filename
            # unzip file while keeping original
            unzip_file(path + filename)

            try:
                input_filename = filename[:-3]
                print 'reading %s' % input_filename
                with open(input_filename, 'rt') as in_file:
                    #with open(output_filename, 'wt') as out_file:
                        ip_24_ranges = defaultdict(int)
                        ip_16_ranges = defaultdict(int)
                        ip_8_ranges = defaultdict(int)

                        http_codes = defaultdict(int)

                        request_names = defaultdict(int)

                        body_bytes_sizes = defaultdict(int)

                        content_version = defaultdict(int)

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

                            body_bytes_sizes[(int(request['body_bytes_sent']) / 1000000) * 1000000] += 1

                            if request['request_uri'][-3:] == '.ts' or request['request_uri'][-5:] == '.m3u8' or request['request_uri'][-7:] == '.webvtt':
                                content_version[''.join(request['request_uri'].rpartition('manifest')[1:])] += 1
                            elif '.mp4?' in request['request_uri'] or '.m3u8?' in request['request_uri']:
                                content_version[request['request_uri'].rpartition('?')[0].rpartition('-')[2]] += 1
                            elif request['request_uri'] == '/healthcheck':
                                content_version[request['request_uri']] += 1
                            elif '/Fragments(' in request['request_uri']:
                                content_version['/Fragments'] += 1
                            elif request['request_uri'][-16:] == '-manifest_hq.mpd' or 'manifest_hq.mpd?' in request['request_uri']:
                                content_version['manifest_hq.mpd'] += 1
                            elif request['request_uri'][-8:].lower() == "manifest" or 'manifest?' in request['request_uri']:
                                content_version['Manifest'] += 1
                            elif request['request_uri'] == '-':
                                content_version['-'] += 1
                            elif request['request_uri'] == '/':
                                content_version['/'] += 1
                            else:
                                print request
                                print line



                print len(ip_8_ranges), len(ip_16_ranges), len(ip_24_ranges)
                print http_codes
                print request_names
                print body_bytes_sizes
                print content_version

            finally:
                print 'deleting %s' % input_filename
                # delete unzipped file - original still exists
                os.remove(path + input_filename)


def main():
    analyze('./', 27, 1, 2016)

if __name__ == "__main__":
    main()
