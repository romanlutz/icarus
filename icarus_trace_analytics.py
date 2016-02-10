from resources.trace_data_analytics import trace_analytics
from resources.beladys_algorithm import beladys_algorithm
import csv

def main():
    traces = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 0
        for line in csv_reader:
            i += 1
            if i >= 29 or i <= 5:
                traces.append(line[0])

    trace_analytics(traces, 'plots')
    # beladys_algorithm([1000], traces)


if __name__ == "__main__":
    main()