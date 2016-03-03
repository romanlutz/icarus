__author__ = 'romanlutz'

import csv

def reformat(filename):
    requests = []
    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        contents = {}
        unique_contents = 0
        for row in csv_reader:
            parts = ' '.join(row).split()
            request = parts[6]
            if request[:6] == '/watch':
                index1 = parts[6].find('?v=')
                index2 = parts[6].find('&v=')
                index3 = parts[6].find('%20v=')

                if index1 != -1:
                    content = parts[6][index1+3:index1+14]
                elif index2 != -1:
                    content = parts[6][index2+3:index2+14]
                elif index3 != -1:
                    content = parts[6][index3+5:index3+16]
                else:
                    print 'error: unexpected format'
                    print parts[6]


                if content in contents:
                    id = contents[content]
                else:
                    unique_contents += 1
                    id = unique_contents
                    contents[content] = id
                event = {'receiver': 0, 'content': id}
                requests.append(event)

    extension = '_reformatted'

    with open(filename[:-4] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        time = 0

        for event in requests:
            writer.writerow((time, event['receiver'], event['content']))
            time += 1


reformat('YouTube_Trace_7days.txt')