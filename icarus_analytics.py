from resources.trace_data_analytics import trace_analytics
from resources.beladys_algorithm import beladys_algorithm
from icarus.results.visualize import draw_stack_deployment
from icarus.registry import TOPOLOGY_FACTORY
import csv

def main():
    traces = []
    trace_lengths = []
    with open('resources/trace_overview.csv', 'r') as trace_file:
        csv_reader = csv.reader(trace_file)
        i = 0
        for line in csv_reader:
            i += 1
            if i == 94:
                traces.append(line[0])
                trace_lengths.append(line[1])

    trace_analytics(traces, trace_lengths, 'plots', do_temporal_distance=True, do_zipf_estimation=False,
                    do_rank_and_occurrence_evolution=True, rank_and_occurrence_evolution_top_n=10,
                    rank_and_occurrence_evolution_interval_size=100000, min_interval_size=8000)
    #beladys_algorithm([1000], traces)


    topologies = {
        'PATH': {'n': [3, 5, 7, 9]},
        'TREE': {'k': [2, 2, 2, 2, 4, 4], 'h': [2, 3, 4, 5, 2, 3]},
        'GEANT': {},
        'GEANT_2': {},
        'WIDE': {},
        'GARR': {},
        'GARR_2': {},
        #'TISCALI': {},
        #'TISCALI_2': {}
    }
    # draw_stack_deployment_for_all_topologies(topologies=topologies)


def draw_stack_deployment_for_all_topologies(topologies):
    for topology_name in topologies:
        try:
            if topologies[topology_name]:
                for config_index in range(len(list(topologies[topology_name].values())[0])):
                    config_params = {}
                    config_param_string = ''
                    for param_name in topologies[topology_name]:
                        config_params[param_name] = topologies[topology_name][param_name][config_index]
                        config_param_string += '-%s=%s' % (param_name, str(config_params[param_name]))
                    topology = TOPOLOGY_FACTORY[topology_name](**config_params)
                    draw_stack_deployment(topology, '%s%s.pdf' % (topology_name.lower(), config_param_string), './plots/topologies')
            else:
                draw_stack_deployment(TOPOLOGY_FACTORY[topology_name](), '%s.pdf' % topology_name.lower(), './plots/topologies')
        except:
            pass

if __name__ == "__main__":
    main()