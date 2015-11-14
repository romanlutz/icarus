__author__ = 'romanlutz'

import csv

def reformat(filename, omit_first_column = False):
    if omit_first_column:
        column = 1
    else:
        column = 0

    requests = []
    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=' ')

        for row in csv_reader:
            content = row[column]

            event = {'receiver': 0, 'content': content}
            requests.append(event)

    extension = '_reformatted'

    with open(filename[:-6] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        time = 0

        for event in requests:
            writer.writerow((time, event['receiver'], event['content']))
            time += 1


for i in range(1, 11):
    reformat('mult_zip_run%d.trace' % i, omit_first_column=True)

reformat('zip0.8.trace')
reformat('zip0.8_300k_requests.trace')