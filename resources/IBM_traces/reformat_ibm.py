__author__ = 'romanlutz'

import csv

def reformat(filename):
    requests = []
    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = row[2]
            time = row[0]

            event = {'receiver': 0, 'content': content, 'time': time}
            requests.append(event)

    extension = '_reformatted'

    with open(filename[:-4] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)

        for event in requests:
            writer.writerow((event['time'], event['receiver'], event['content']))


reformat('anon-url-trace.txt')
