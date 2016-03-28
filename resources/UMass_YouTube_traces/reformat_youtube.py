from datetime import datetime, timedelta
import csv


def time_difference(timestamp1, timestamp2):
    month1, _, rest = timestamp1.partition(' ')
    day1, _, rest = rest.partition(' ')
    year1, _, rest = rest.partition(' ')
    hour1, _, rest = rest.partition(':')
    min1, _, sec1 = rest.partition(':')
    sec1, _, microsec1 = sec1.partition('.')
    microsec1 = microsec1[:6]

    month2, _, rest = timestamp2.partition(' ')
    day2, _, rest = rest.partition(' ')
    year2, _, rest = rest.partition(' ')
    hour2, _, rest = rest.partition(':')
    min2, _, sec2 = rest.partition(':')
    sec2, _, microsec2 = sec2.partition('.')
    microsec2 = microsec2[:6]
    [year1, year2, day1, day2, hour1, hour2, min1, min2, sec1, sec2, microsec1, microsec2] = \
        map(int, [year1, year2, day1, day2, hour1, hour2, min1, min2, sec1, sec2, microsec1, microsec2])

    # there are only January and Feburary requests, so this simple rule works
    month1 = 1 if month1 == 'Jan' else 2
    month2 = 1 if month2 == 'Jan' else 2

    time1 = datetime(year1, month1, day1, hour1, min1, sec1, microsec1)
    time2 = datetime(year2, month2, day2, hour2, min2, sec2, microsec2)

    if time2 > time1:
        delta = time2 - time1
    else:
        delta = time1 - time2

    if delta.days == 0:
        return delta.seconds
    else:
        return 86400


def reformat(filename, no_duplicates=False):
    requests = []
    duplicate_requests = 0 # only within 10 seconds

    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        contents = {}
        request_log = {} # id - ip - last request time
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

                if no_duplicates:
                    if content in contents:
                        id = contents[content]
                    else:
                        unique_contents += 1
                        id = unique_contents
                        contents[content] = id
                        request_log[id] = {}
                    event = {'receiver': 0, 'content': id}

                    ip = parts[4]
                    timestamp = ' '.join([parts[0], parts[1][:2], parts[2], parts[3]])

                    if ip in request_log[id]:
                        # this ID was requested by this IP before
                        if time_difference(request_log[id][ip], timestamp) < 10:
                            # duplicate request, ignore it
                            duplicate_requests += 1
                        else:
                            requests.append(event)
                    else:
                        requests.append(event)

                    request_log[id][ip] = timestamp

                else:
                    if content in contents:
                        id = contents[content]
                    else:
                        unique_contents += 1
                        id = unique_contents
                        contents[content] = id

                    event = {'receiver': 0, 'content': id}
                    requests.append(event)

    extension = '_no_duplicates_reformatted' if no_duplicates else '_reformatted'

    if no_duplicates:
        print 'duplicate requests:', duplicate_requests

    with open(filename[:-4] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        time = 0

        for event in requests:
            writer.writerow((time, event['receiver'], event['content']))
            time += 1


'''
reformat('YouTube_Trace_7days.txt')
reformat('YouTube_Trace_7days.txt', True)
'''