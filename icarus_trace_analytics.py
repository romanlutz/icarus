from resources.trace_data_analytics import trace_analytics
from resources.beladys_algorithm import beladys_algorithm
import csv

def main():
    traces = []
    trace_lengths = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 0
        for line in csv_reader:
            i += 1
            if i <= 1:
                traces.append(line[0])
                trace_lengths.append(line[1])

    trace_analytics(traces, trace_lengths, 'plots', do_temporal_distance=True, do_zipf_estimation=False,
                    do_rank_and_occurrence_evolution=False, rank_and_occurrence_evolution_top_n=10,
                    rank_and_occurrence_evolution_interval_size=100000, min_interval_size=8000)
    #beladys_algorithm([1000], traces)


if __name__ == "__main__":
    main()