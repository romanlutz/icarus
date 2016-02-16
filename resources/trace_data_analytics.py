import csv
from icarus.tools import zipf_fit
import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from textwrap import wrap


"""
The purpose of this program is to check standardized traces for the total number of requests, the number of unique
elements and possibly other interesting data in the future if required.
The standard format of a row in a trace is: time, receiver, object
For example: 10.3, 0, 15 means that after 10.3 time units object 15 was requested by host 0
"""



def trace_analytics(traces, plotdir, min_interval_size=2000, zipf_estimation=True, average_distance=True):
    print 'starting preliminary analysis'
    properties = ['requests', 'objects', 'single_occurrences', 'average_distance']
    data = {}
    for trace_path in traces:
        with open('resources/' + trace_path, 'r') as trace:
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
            if zipf_estimation:
                occurrence_aggregate = {} # for later Zipfian MLE
                for i in range(requests/min_interval_size + (1 if requests % min_interval_size != 0 else 0)):
                    occurrence_aggregate[i] = []
            single_occurrences = 0
            for object in occurrences:
                # the following part is about aggregating the occurrences for all interval splits in the trace
                # initialize as zero occurrences
                if zipf_estimation:
                    for i in occurrence_aggregate:
                        occurrence_aggregate[i].append(0)
                    # add 1 to the counter in the corresponding bucket
                    for occurrence in occurrences[object]:
                        occurrence_aggregate[occurrence/2000][-1] += 1

                if average_distance:
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

            if average_distance:
                overall_average_distance = 0
                consecutive_request_pairs = 0
                for n in occurrence_distribution:
                    overall_average_distance += \
                        occurrence_distribution[n]['counter'] * occurrence_distribution[n]['average_distance'] * (n-1)
                    consecutive_request_pairs += occurrence_distribution[n]['counter'] * (n-1)

                overall_average_distance /= consecutive_request_pairs

            data[trace_path] = {'requests': requests, 'objects': len(occurrences)}
            if average_distance:
                data[trace_path]['single_occurrences'] = single_occurrences
                data[trace_path]['average_distance'] = overall_average_distance

            if zipf_estimation:
                occurrence_aggregate_length = len(occurrence_aggregate)
                covered_intervals = [False]*occurrence_aggregate_length
                for start_interval in range(0, occurrence_aggregate_length):
                    if not covered_intervals[start_interval]:
                        for number_of_merged_intervals in range(occurrence_aggregate_length - start_interval, 0, -1):
                            if not covered_intervals[start_interval]:
                                aggregate = occurrence_aggregate[start_interval]
                                for i in range(start_interval+1, start_interval + number_of_merged_intervals):
                                    aggregate = [x+y for (x,y) in zip(aggregate, occurrence_aggregate[i])]

                                try:
                                    zipf_alpha, zipf_fit_prob = zipf_fit(aggregate, need_sorting=True)
                                    if zipf_fit_prob > 0.95:
                                        for i in range(start_interval, start_interval + number_of_merged_intervals):
                                            covered_intervals[i] = True
                                        data[trace_path]['zipf_alpha %d %d' % (start_interval, number_of_merged_intervals)] = zipf_alpha
                                        data[trace_path]['zipf_fit_prob %d %d' % (start_interval, number_of_merged_intervals)] = zipf_fit_prob
                                except:
                                    pass

        print trace_path, 'done'
    print 'preliminary analysis done'

    for property in properties:
        for trace in traces:
            print data[trace][property], '\t',
        print ''

    if zipf_estimation:
        for trace in traces:
            path = os.path.join(plotdir, trace[:-6] + '_zipf_intervals.pdf')
            # ensure the path exists and create it if necessary
            directories = path.split('/')[1:-1]
            current_path = plotdir
            for directory in directories:
                current_path += '/' + directory
                if not os.path.isdir(current_path):
                    os.makedirs(current_path)

            pdf=PdfPages(path)
            fig = plt.figure()

            for entry in data[trace]:
                if entry[:13] == 'zipf_fit_prob':
                    entry_parts = entry.split(' ')
                    start_interval = int(entry_parts[1])
                    interval_length = int(entry_parts[2])
                    alpha = data[trace]['zipf_alpha %d %d' % (start_interval, interval_length)]
                    p = plt.plot([start_interval*min_interval_size, (start_interval+interval_length)*min_interval_size],
                                 [alpha, alpha], '-', linewidth=2, color='r')

            plt.xlabel('requests')
            plt.ylabel('Zipf alpha')
            title = 'Estimation of Zipf alpha parameter'
            plt.title("\n".join(wrap(title, 60)))

            pdf.savefig(fig)
            pdf.close()
            plt.close()
