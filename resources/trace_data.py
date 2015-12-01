import csv

"""
The purpose of this program is to check standardized traces for the total number of requests, the number of unique
elements and possibly other interesting data in the future if required.
The standard format of a row in a trace is: time, receiver, object
For example: 10.3, 0, 15 means that after 10.3 time units object 15 was requested by host 0
"""

traces = []
with open('trace_overview.csv', 'r') as trace_file:
    csv_reader = csv.reader(trace_file)
    for line in csv_reader:
        traces.append(line[0])

data = {}
for trace_path in traces:
    with open(trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        requests = 0
        objects = []

        for line in csv_reader:
            time, receiver, object = line[0], line[1], line[2]
            requests += 1
            if object not in objects:
                objects.append(object)

        data[trace_path] = {'requests': requests, 'objects': len(objects)}

print traces
for trace in traces:
    print data[trace]['requests'], '\t',
print ''
for trace in traces:
    print data[trace]['objects'], '\t',
print ''


