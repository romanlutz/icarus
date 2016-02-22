import csv
from icarus.tools import zipf_fit
import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from textwrap import wrap
import numpy as np
from icarus.results import create_path_if_necessary


"""
The purpose of this program is to check standardized traces for the total number of requests, the number of unique
elements and possibly other interesting data in the future if required.
The standard format of a row in a trace is: time, receiver, object
For example: 10.3, 0, 15 means that after 10.3 time units object 15 was requested by host 0
"""



def trace_analytics(traces, trace_lengths, plotdir, min_interval_size=2000, zipf_estimation=True, average_distance=True,
                    rank_and_occurrence_evolution=True, rank_and_occurrence_evolution_top_n=10,
                    rank_and_occurrence_evolution_intervals=10):
    print 'starting preliminary analysis'
    properties = ['requests', 'objects', 'single_occurrences', 'average_distance']
    data = {}

    for trace_index, trace_path in enumerate(traces):
        with open('resources/' + trace_path, 'r') as trace:
            csv_reader = csv.reader(trace)

            if rank_and_occurrence_evolution:
                occurrences_to_id_map = {}

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

                total_occurrence_count = len(occurrences[object])

                if rank_and_occurrence_evolution:
                    if total_occurrence_count in occurrences_to_id_map:
                        occurrences_to_id_map[total_occurrence_count].append(object)
                    else:
                        occurrences_to_id_map[total_occurrence_count] = [object]

                if average_distance:
                    # the following parts are for checking the temporal distance
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

        if rank_and_occurrence_evolution:
            # determine top elements of the whole trace based on total occurrence
            top_n_ids = []
            while len(top_n_ids) < rank_and_occurrence_evolution_top_n:
                max_occ = max(occurrences_to_id_map.keys())
                top_n_ids.append(occurrences_to_id_map[max_occ][0])
                occurrences_to_id_map[max_occ] = occurrences_to_id_map[max_occ][1:]
                if occurrences_to_id_map[max_occ] == []:
                    del occurrences_to_id_map[max_occ]

            del occurrences_to_id_map

            # divide trace into bins
            intervals = np.linspace(0, int(trace_lengths[trace_index]) + 1, rank_and_occurrence_evolution_intervals + 1)

            # bin_top_occurrences[bin][occurrence counter] contains a list of all elements that have the given
            # occurrences counter in the specified bin; this is necessary for the rank evolution
            bin_top_occurrences = {}
            for bin in range(rank_and_occurrence_evolution_intervals):
                bin_top_occurrences[bin] = {}

            # occurrence_evolution[id] contains a list for the element with the specified id containing all occurrence
            # counters for the bins
            occurrence_evolution = {}

            # bin_occurrences contains all the counters of the top elements for every interval
            # it will be sorted later and will be the basis for bin_in_between_occurrences
            bin_occurrences = []
            for _ in range(rank_and_occurrence_evolution_intervals):
                bin_occurrences.append([])
            for id in top_n_ids:
                occurrence_evolution[id] = []
                for interval_index in range(len(intervals) - 1):
                    lower_bound = intervals[interval_index]
                    upper_bound = intervals[interval_index + 1]

                    interval_occ = len(filter(lambda x: x < upper_bound and x >= lower_bound, occurrences[id]))

                    occurrence_evolution[id].append(interval_occ)

                    if interval_occ in bin_top_occurrences[interval_index]:
                        bin_top_occurrences[interval_index][interval_occ].append(id)
                    else:
                        bin_top_occurrences[interval_index][interval_occ] = [id]

                    bin_occurrences[interval_index].append(interval_occ)

            for bin_occ in bin_occurrences:
                bin_occ.sort()

            data[trace_path]['occurrence_evolution'] = occurrence_evolution

            # for every other element that occurred more than any of the top n elements
            # we note down in between which top elements it ranks for a specific bin
            # bin_in_between_occurrences[interval][i] would be the number of non-top elements that rank higher than
            # the (n-i)-th ranked top element but lower than the (n-i-1)th ranked top element
            bin_in_between_occurrences = []
            for _ in range(rank_and_occurrence_evolution_intervals):
                bin_in_between_occurrences.append([0]*rank_and_occurrence_evolution_top_n)
            for id in occurrences:
                if id not in top_n_ids:
                    for interval_index in range(len(intervals) - 1):
                        lower_bound = intervals[interval_index]
                        upper_bound = intervals[interval_index + 1]
                        interval_occ = len(filter(lambda x: x < upper_bound and x >= lower_bound, occurrences[id]))

                        # determine the lowest ranked top element which is ranked above the current element
                        i = 0
                        while i < rank_and_occurrence_evolution_top_n and interval_occ > bin_occurrences[interval_index][i]:
                            i += 1

                        # i == 0 means that the current element is ranked lower than any top element
                        if i > 0:
                            # index i in bin_in_between_occurrences means its rank is larger than top element at index i
                            # but smaller than top element at index i+1
                            bin_in_between_occurrences[interval_index][i-1] += 1

            ranks = {}
            for id in top_n_ids:
                ranks[id] = []
            for interval_index in range(rank_and_occurrence_evolution_intervals):
                rank = 1
                top_occ_order = bin_top_occurrences[interval_index].keys()
                top_occ_order.sort()

                j = len(bin_in_between_occurrences[interval_index]) - 1
                while j >= 0:
                    rank += bin_in_between_occurrences[interval_index][j]

                    # treat all ids with the same occurrence counter in this interval in one iteration
                    occ = top_occ_order[-1]
                    top_occ_order = top_occ_order[:-1]
                    ids = bin_top_occurrences[interval_index][occ]
                    for id in ids:
                        ranks[id].append(rank)
                    rank += len(ids)
                    j -= len(ids)

            data[trace_path]['rank_evolution'] = ranks

        print trace_path, 'done'
    print 'preliminary analysis done'

    for property in properties:
        for trace in traces:
            print data[trace][property], '\t',
        print ''

    if zipf_estimation:
        for trace in traces:
            path = os.path.join(plotdir, trace[:-6] + '_zipf_intervals.pdf')
            create_path_if_necessary(path, plotdir)

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

    if rank_and_occurrence_evolution:
        for trace in traces:
            # rank evolution
            path = os.path.join(plotdir, trace[:-6] + '_rank_evolution.pdf')
            create_path_if_necessary(path, plotdir)

            pdf=PdfPages(path)
            fig = plt.figure()

            for entry in data[trace]:
                if entry == 'rank_evolution':
                    ranks = data[trace][entry]
                    ids = ranks.keys()
                    for id in ids:
                        p = plt.plot(range(1, rank_and_occurrence_evolution_intervals + 1), ranks[id], '-', linewidth=2)

            plt.xlabel('intervals')
            plt.ylabel('rank')
            title = 'Rank evolution for top elements'
            plt.title("\n".join(wrap(title, 60)))
            plt.gca().invert_yaxis()
            plt.yscale('log')

            pdf.savefig(fig)
            pdf.close()
            plt.close()

            # occurrence evolution plot
            path = os.path.join(plotdir, trace[:-6] + '_occurrence_evolution.pdf')
            create_path_if_necessary(path, plotdir)

            pdf=PdfPages(path)
            fig = plt.figure()

            for entry in data[trace]:
                if entry == 'occurrence_evolution':
                    occurrence_evolution = data[trace][entry]
                    ids = occurrence_evolution.keys()
                    for id in ids:
                        p = plt.plot(range(1, rank_and_occurrence_evolution_intervals + 1), occurrence_evolution[id],
                                     '-', linewidth=2)

            plt.xlabel('intervals')
            plt.ylabel('occurrences')
            title = 'Occurrence evolution for top elements'
            plt.title("\n".join(wrap(title, 60)))

            pdf.savefig(fig)
            pdf.close()
            plt.close()


