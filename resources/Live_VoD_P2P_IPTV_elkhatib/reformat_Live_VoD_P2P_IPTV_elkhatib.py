__author__ = 'romanlutz'

import csv
import datetime

def reformat(filename, columnTime, columnIP, columnName, one_cache_scenario=True):
    requests = {}

    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        first = True
        receivers = {}
        receiver_id = 0
        contents = {}
        content_id = 0

        for row in csv_reader:
            # exclude first line
            if first:
                first = False
            else:
                time = row[columnTime].split()
                [year, month, day] = list(map(int, time[0].split('-')))
                [hour, minute, second] = list(map(int, time[1].split(':')))
                ip = row[columnIP]
                content = row[columnName]

                event = {'receiver': ip, 'content': content}

                # map IP addresses to unique IDs
                if ip in list(receivers.keys()):
                    pass
                else:
                    receivers[ip] = receiver_id
                    receiver_id += 1

                # map content names to unique IDs
                if content in list(contents.keys()):
                    pass
                else:
                    contents[content] = content_id
                    content_id += 1

                if year not in list(requests.keys()):
                    requests[year] = {month: {day: {hour: {minute: {second: [event]}}}}}
                elif month not in list(requests[year].keys()):
                    requests[year][month] = {day: {hour: {minute: {second: [event]}}}}
                elif day not in list(requests[year][month].keys()):
                    requests[year][month][day] = {hour: {minute: {second: [event]}}}
                elif hour not in list(requests[year][month][day].keys()):
                    requests[year][month][day][hour] = {minute: {second: [event]}}
                elif minute not in list(requests[year][month][day][hour].keys()):
                    requests[year][month][day][hour][minute] = {second: [event]}
                elif second not in list(requests[year][month][day][hour][minute].keys()):
                    requests[year][month][day][hour][minute][second] = [event]
                else:
                    requests[year][month][day][hour][minute][second].append(event)

    min_time = {}
    min_time['year']  = min(requests.keys())
    min_time['month'] = min(requests[min_time['year']].keys())
    min_time['day']   = min(requests[min_time['year']][min_time['month']].keys())
    min_time['hour']  = min(requests[min_time['year']][min_time['month']][min_time['day']].keys())
    min_time['minute'] = min(requests[min_time['year']][min_time['month']][min_time['day']][min_time['hour']].keys())
    min_time['second'] = min(requests[min_time['year']][min_time['month']][min_time['day']][min_time['hour']][min_time['minute']].keys())

    extension = '_one_cache_scenario' if one_cache_scenario else ''

    with open(filename[:-4] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        years = list(requests.keys())
        years.sort()
        for year in years:
            months = list(requests[year].keys())
            months.sort()
            for month in months:
                days = list(requests[year][month].keys())
                days.sort()
                for day in days:
                    hours = list(requests[year][month][day].keys())
                    hours.sort()
                    for hour in hours:
                        minutes = list(requests[year][month][day][hour].keys())
                        minutes.sort()
                        for minute in minutes:
                            seconds = list(requests[year][month][day][hour][minute].keys())
                            seconds.sort()
                            for second in seconds:
                                for event in requests[year][month][day][hour][minute][second]:
                                    time = timeDifference(min_time, year, month, day, hour, minute, second)
                                    if one_cache_scenario:
                                        writer.writerow((time, 0, contents[event['content']]))
                                    else:
                                        writer.writerow((time, receivers[event['receiver']], contents[event['content']]))

def timeDifference(start_time, year, month, day, hour, minute, second):
    return (datetime.datetime(year, month, day, hour, minute, second)
           - datetime.datetime(start_time['year'], start_time['month'], start_time['day'],
                               start_time['hour'], start_time['minute'], start_time['second'])).total_seconds()

'''
reformat('NextSharePC.csv', 2, 9, 13)
reformat('NextShareTV.csv', 2, 8, 12)
reformat('NextSharePC.csv', 2, 9, 13, one_cache_scenario=False)
reformat('NextShareTV.csv', 2, 8, 12, one_cache_scenario=False)
'''