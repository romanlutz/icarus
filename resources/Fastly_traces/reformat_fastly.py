__author__ = 'romanlutz'

import csv

def reformat(filename, size_given=False, threshold=10000):

    if filename == 'requests.txt':
        n_requests = 14885146
    elif filename == 'requests-1M-2016-3-15.txt':
        n_requests = 1007545
    else:
        raise ValueError('file name incorrect')

    output_filename = filename[:-4] + '_reformatted.trace'
    with open(output_filename, 'wb') as write_file:
        writer = csv.writer(write_file, quoting = csv.QUOTE_NONE)
        with open(filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=' ')
            contents = {}
            content_id = 0
            i = 1

            file_sizes_below_threshold = 0
            file_sizes_above_threshold = 0

            for row in csv_reader:
                time = row[0]
                content = row[1]
                i+=1

                if size_given:
                    size = int(row[2])
                    if size > threshold:
                        file_sizes_above_threshold += 1
                        if content not in contents:
                            content_id += 1
                            contents[content] = content_id
                        writer.writerow((time, 0, contents[content]))
                    else:
                        file_sizes_below_threshold += 1
                else:
                    if content not in contents:
                        content_id += 1
                        contents[content] = content_id
                    writer.writerow((time, 0, contents[content]))

                if i % 10000 == 0:
                    print float(i)/float(n_requests), len(contents)

            print 'file sizes below threshold:', file_sizes_below_threshold
            print 'file sizes above threshold:', file_sizes_above_threshold

'''
reformat('requests.txt')
reformat('requests-1M-2016-3-15.txt', False)
'''