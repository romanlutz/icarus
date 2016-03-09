__author__ = 'romanlutz'

import csv

def reformat(filename):
    output_filename = filename[:-4] + '_reformatted.trace'
    with open(output_filename, 'wb') as write_file:
        writer = csv.writer(write_file, quoting = csv.QUOTE_NONE)
        with open(filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=' ')
            contents = {}
            content_id = 0
            i = 1

            for row in csv_reader:
                time = row[0]
                content = row[1]
                i+=1

                if content not in contents:
                    content_id += 1
                    contents[content] = content_id
                writer.writerow((time, 0, contents[content]))

                if i % 10000 == 0:
                    print float(i)/float(14885146), len(contents)

'''
reformat('requests.txt')
'''