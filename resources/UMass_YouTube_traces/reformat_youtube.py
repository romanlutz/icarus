__author__ = 'romanlutz'

import csv

def reformat(filename):
    requests = []
    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = row[0]

            event = {'receiver': 0, 'content': content}
            requests.append(event)

    extension = '_reformatted'

    with open(filename[:-6] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        time = 0

        for event in requests:
            writer.writerow((time, event['receiver'], event['content']))
            time += 1


reformat('requests_full_youtube.trace')
reformat('requests_youtube.trace')