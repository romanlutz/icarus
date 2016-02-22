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
            traces.append(line[0])
            trace_lengths.append(line[1])

    trace_analytics(traces, trace_lengths, 'plots', zipf_estimation=False, rank_and_occurrence_evolution=True,
                    rank_and_occurrence_evolution_top_n=10, rank_and_occurrence_evolution_intervals=10)
    # beladys_algorithm([1000], traces)


if __name__ == "__main__":
    main()