import csv
from collections import defaultdict

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


def trace_analytics(traces, trace_lengths, plotdir, min_interval_size=2000, do_zipf_estimation=True,
                    do_temporal_distance=True, do_rank_and_occurrence_evolution=True,
                    rank_and_occurrence_evolution_top_n=10, rank_and_occurrence_evolution_intervals=10):
    print 'starting preliminary analysis'
    properties = ['requests', 'objects', 'single_occurrences', 'average_distance']
    data = {}

    for trace_index, trace_path in enumerate(traces):
        print trace_path

        if do_zipf_estimation:
            plot_zipf_data(zipf_estimation(trace_path, min_interval_size), plotdir, trace_path, min_interval_size)

        if do_rank_and_occurrence_evolution:
            plot_rank_and_occurrence_evolution(trace_path, plotdir,
                                               rank_and_occurrence_evolution(trace_path,
                                                                                int(trace_lengths[trace_index]),
                                                                                rank_and_occurrence_evolution_top_n,
                                                                                rank_and_occurrence_evolution_intervals),
                                               rank_and_occurrence_evolution_intervals)

        if do_temporal_distance:
            data[trace_path] = temporal_distance(trace_path)



    for property in properties:
        for trace in traces:
            print data[trace][property], '\t',
        print ''


def zipf_estimation(trace_path, min_interval_size=2000):
    print 'estimating zipfian alpha parameter for all intervals of the trace'
    data = []
    with open('resources/' + trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        requests = 0
        occurrence_aggregate = defaultdict(int)

        for index, line in enumerate(csv_reader):
            time, receiver, object = line[0], line[1], line[2]
            requests += 1

            occurrence_aggregate[object] += 1

            if requests % min_interval_size == 0:
                zipf_alpha, zipf_fit_prob = zipf_fit(occurrence_aggregate.values(), need_sorting=True)
                if zipf_fit_prob > 0.95:
                    data.append(zipf_alpha)
                else:
                    data.append(None)
                occurrence_aggregate.clear()

    return data


def plot_zipf_data(data, plotdir, trace, min_interval_size=2000):
    print 'plotting zipfian alpha parameters'
    path = os.path.join(plotdir, trace[:-6] + '_zipf_intervals.pdf')
    create_path_if_necessary(path, plotdir)

    pdf = PdfPages(path)
    fig = plt.figure()

    for interval_index, alpha in enumerate(data):
        if alpha is not None:
            p = plt.plot([(interval_index - 1) * min_interval_size, interval_index * min_interval_size],
                         [alpha, alpha], '-', linewidth=2, color='r')

    plt.xlabel('requests')
    plt.ylabel('Zipf alpha')
    title = 'Estimation of Zipf alpha parameter'
    plt.title("\n".join(wrap(title, 60)))

    pdf.savefig(fig)
    pdf.close()
    plt.close()


def rank_and_occurrence_evolution(trace_path, trace_length, rank_and_occurrence_evolution_top_n=10,
                                  rank_and_occurrence_evolution_intervals=10):
    print 'computing rank and occurrence evolution'
    data = {}

    with open('resources/' + trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        total_occurrences_to_id_map = defaultdict(list)

        # divide trace into bins
        intervals = np.linspace(0, trace_length, rank_and_occurrence_evolution_intervals + 1)
        interval_index = 0

        requests = 0
        occurrences = defaultdict(lambda: [0] * rank_and_occurrence_evolution_intervals)

        for index, line in enumerate(csv_reader):
            time, receiver, object = line[0], line[1], line[2]
            requests += 1
            if requests > intervals[interval_index + 1]:
                interval_index += 1

            occurrences[object][interval_index] += 1

        for object in occurrences:
            total_occurrences_to_id_map[sum(occurrences[object])].append(object)

    # determine top elements of the whole trace based on total occurrence
    top_n_ids = []
    while len(top_n_ids) < rank_and_occurrence_evolution_top_n:
        max_occ = max(total_occurrences_to_id_map.keys())
        top_n_ids.append(total_occurrences_to_id_map[max_occ][0])
        total_occurrences_to_id_map[max_occ] = total_occurrences_to_id_map[max_occ][1:]
        if total_occurrences_to_id_map[max_occ] == []:
            del total_occurrences_to_id_map[max_occ]

    del total_occurrences_to_id_map

    data['occurrence_evolution'] = {}
    for id in top_n_ids:
        data['occurrence_evolution'][id] = occurrences[id]

    # determine rank by getting all counters and sorting them
    ranks = defaultdict(list)
    for interval_index in range(rank_and_occurrence_evolution_intervals):
        interval_occurrence_counters = [occurrences[id][interval_index] for id in occurrences]
        interval_occurrence_counters.sort(reverse=True)

        for id in top_n_ids:
            ranks[id].append(interval_occurrence_counters.index(occurrences[id][interval_index]))

    data['rank_evolution'] = ranks

    return data


def plot_rank_and_occurrence_evolution(trace, plotdir, data, rank_and_occurrence_evolution_intervals):
    print 'plotting rank and occurrence evolution'

    path = os.path.join(plotdir, trace[:-6] + '_rank_evolution.pdf')
    create_path_if_necessary(path, plotdir)

    pdf = PdfPages(path)
    fig = plt.figure()

    ranks = data['rank_evolution']
    ids = ranks.keys()

    cmap = plt.get_cmap('jet')
    norm = plt.normalize(0, len(ids) - 1)

    for index, id in enumerate(ids):
        p = plt.plot(range(1, rank_and_occurrence_evolution_intervals + 1), ranks[id], '-', linewidth=2, color=cmap(norm(index)))

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

    pdf = PdfPages(path)
    fig = plt.figure()

    occurrence_evolution = data['occurrence_evolution']
    ids = occurrence_evolution.keys()
    for index, id in enumerate(ids):
        p = plt.plot(range(1, rank_and_occurrence_evolution_intervals + 1), occurrence_evolution[id], '-', linewidth=2, color=cmap(norm(index)))

    plt.xlabel('intervals')
    plt.ylabel('occurrences')
    title = 'Occurrence evolution for top elements'
    plt.title("\n".join(wrap(title, 60)))

    pdf.savefig(fig)
    pdf.close()
    plt.close()


def temporal_distance(trace_path):
    print 'computing average temporal distance'

    with open('resources/' + trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        requests = 0
        first_occurrence = {}
        last_occurrence = {}
        total_occurrences = defaultdict(int)

        for index, line in enumerate(csv_reader):
            time, receiver, object = line[0], line[1], line[2]
            requests += 1

            if object not in first_occurrence:
                first_occurrence[object] = requests

            last_occurrence[object] = requests
            total_occurrences[object] += 1

        single_occurrences = sum([1 if last_occurrence[id] == first_occurrence[id] else 0 for id in total_occurrences])
        # the formula for temporal distance is as follows: consider only elements with multiple occurrences
        # (sum over difference between element's first and last occurrence) / (number of element's occurrences - 1)
        total_distance = \
            sum([last_occurrence[id] - first_occurrence[id] + 1 - total_occurrences[id] for id in total_occurrences])
        total_pairs_count = sum([x-1 for x in total_occurrences.values()])

    return {'requests': requests, 'objects': len(total_occurrences), 'single_occurrences': single_occurrences,
            'average_distance': float(total_distance) / float(total_pairs_count)}