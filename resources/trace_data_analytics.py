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
        if (i >= 31 or i <= 7) and i == 3:
            traces.append(line[0])

properties = ['requests', 'objects', 'single_occurrences', 'average_distance']
zipf_properties = []
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
        min_interval_size = 2000
        occurrence_aggregate = {} # for later Zipfian MLE
        for i in range(requests/min_interval_size + (1 if requests % min_interval_size != 0 else 0)):
            occurrence_aggregate[i] = []
        single_occurrences = 0
        for object in occurrences:
            # the following part is about aggregating the occurrences for all interval splits in the trace
            # initialize as zero occurrences
            for i in occurrence_aggregate:
                occurrence_aggregate[i].append(0)
            # add 1 to the counter in the corresponding bucket
            for occurrence in occurrences[object]:
                occurrence_aggregate[occurrence/2000][-1] += 1

            # the following parts are for checking the temporal distance
            total_occurrence_count = len(occurrences[object])
            if total_occurrence_count > 1:
                if total_occurrence_count in occurrence_distribution:
                    previous_counter = occurrence_distribution[total_occurrence_count]['counter']
                    previous_average_distance = occurrence_distribution[total_occurrence_count]['average_distance']
                    occurrence_distribution[total_occurrence_count]['counter'] += 1
                    occurrence_distribution[total_occurrence_count]['average_distance'] = \
                        (previous_average_distance * previous_counter +
                         float(occurrences[object][-1] - occurrences[object][0] - total_occurrence_count + 1) /
                         float(total_occurrence_count - 1)) / \
                         float(occurrence_distribution[total_occurrence_count]['counter'])
                else:
                    occurrence_distribution[total_occurrence_count] = {}
                    occurrence_distribution[total_occurrence_count]['counter'] = 1
                    occurrence_distribution[total_occurrence_count]['average_distance'] = \
                        float(occurrences[object][-1] - occurrences[object][0] - total_occurrence_count + 1) / \
                        float(total_occurrence_count - 1)
            else:
                single_occurrences += 1

        overall_average_distance = 0
        consecutive_request_pairs = 0
        for n in occurrence_distribution:
            overall_average_distance += \
                occurrence_distribution[n]['counter'] * occurrence_distribution[n]['average_distance'] * (n-1)
            consecutive_request_pairs += occurrence_distribution[n]['counter'] * (n-1)

        overall_average_distance /= consecutive_request_pairs

        data[trace_path] = {'requests': requests, 'objects': len(occurrences), 'single_occurrences': single_occurrences,
                            'average_distance': overall_average_distance}

        occurrence_aggregate_length = len(occurrence_aggregate)
        for number_of_merged_intervals in range(1, occurrence_aggregate_length + 1):
            for start_interval in range(0, len(occurrence_aggregate) + 1 - number_of_merged_intervals):
                aggregate = occurrence_aggregate[start_interval]
                for i in range(start_interval+1, start_interval + number_of_merged_intervals):
                    aggregate = [x+y for (x,y) in zip(aggregate, occurrence_aggregate[i])]

                try:
                    zipf_alpha, zipf_fit_prob = zipf_fit(aggregate, need_sorting=True)
                except:
                    zipf_alpha = 'failed'
                    zipf_fit_prob = 'failed'
                data[trace_path]['zipf_alpha %d %d' % (start_interval, number_of_merged_intervals)] = zipf_alpha
                data[trace_path]['zipf_fit_prob %d %d' % (start_interval, number_of_merged_intervals)] = zipf_fit_prob
                zipf_properties.append('zipf_fit_prob %d %d' % (start_interval, number_of_merged_intervals))
                zipf_properties.append('zipf_alpha %d %d' % (start_interval, number_of_merged_intervals))
                print 'start:', start_interval, 'merged:', number_of_merged_intervals, 'total:', occurrence_aggregate_length

        print data[trace_path]

zipf_properties.reverse()
properties.extend(zipf_properties)

for property in properties:
    print property
for trace in traces:
    print trace
    for property in properties:
        print data[trace][property]
    print ''
