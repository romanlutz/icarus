import csv
from icarus.tools import zipf_fit

"""
The purpose of this program is to check standardized traces for the total number of requests, the number of unique
elements and possibly other interesting data in the future if required.
The standard format of a row in a trace is: time, receiver, object
For example: 10.3, 0, 15 means that after 10.3 time units object 15 was requested by host 0
"""

traces = []
with open('trace_overview.csv', 'r') as trace_file:
    csv_reader = csv.reader(trace_file)
    i = 0
    for line in csv_reader:
        i += 1
        if i >= 31 or i <= 7:
            traces.append(line[0])

data = {}
for trace_path in traces:
    with open(trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        requests = 0
        occurrences = {}

        for index, line in enumerate(csv_reader):
            time, receiver, object = line[0], line[1], line[2]
            requests += 1

            if object in occurrences:
                occurrences[object].append(index)
            else:
                occurrences[object] = [index]

            if requests % 100000 == 0:
                print trace_path, requests

        occurrence_distribution = {}
        occurrence_total = [] # for later Zipfian MLE
        single_occurrences = 0
        for object in occurrences:
            total_occurrences = len(occurrences[object])
            occurrence_total.append(total_occurrences)
            if total_occurrences > 1:
                if total_occurrences in occurrence_distribution:
                    previous_counter = occurrence_distribution[total_occurrences]['counter']
                    previous_average_distance = occurrence_distribution[total_occurrences]['average_distance']
                    occurrence_distribution[total_occurrences]['counter'] += 1
                    occurrence_distribution[total_occurrences]['average_distance'] = \
                        (previous_average_distance * previous_counter + \
                        float(occurrences[object][-1] - occurrences[object][0] - total_occurrences + 1) / \
                        float(total_occurrences - 1)) / float(occurrence_distribution[total_occurrences]['counter'])
                else:
                    occurrence_distribution[total_occurrences] = {}
                    occurrence_distribution[total_occurrences]['counter'] = 1
                    occurrence_distribution[total_occurrences]['average_distance'] = \
                        float(occurrences[object][-1] - occurrences[object][0] - total_occurrences + 1) / \
                        float(total_occurrences - 1)
            else:
                single_occurrences += 1

        overall_average_distance = 0
        consecutive_request_pairs = 0
        for n in occurrence_distribution:
            overall_average_distance += \
                occurrence_distribution[n]['counter'] * occurrence_distribution[n]['average_distance'] * (n-1)
            consecutive_request_pairs += occurrence_distribution[n]['counter'] * (n-1)

        overall_average_distance /= consecutive_request_pairs

        zipf_alpha, zipf_fit_prob = zipf_fit(occurrence_total, need_sorting=True)

        data[trace_path] = {'requests': requests, 'objects': len(occurrences), 'single_occurrences': single_occurrences,
                            'average_distance': overall_average_distance,
                            'zipf_alpha': zipf_alpha, 'zipf_fit_prob': zipf_fit_prob}
        print data[trace_path]


properties = ['requests', 'objects', 'single_occurrences', 'average_distance', 'zipf_alpha', 'zipf_fit_prob']
print traces
for property in properties:
    for trace in traces:
        print data[trace][property], '\t',
    print ''