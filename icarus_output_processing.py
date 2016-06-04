from icarus.results.visualize import draw_cache_level_proportions
from icarus.results.output_results import print_cache_hit_rates, print_results_full, generate_result_evolution_plots

def main():
    file_prefix = 'results-weighted-ibm-c1000'
    #draw_cache_level_proportions('plots', 'results', '.spickle')
    #print_results_full(file_prefix, '.spickle')

    combinations = {'unweighted hit rate': 'MEAN', 'normalized reward': 'WEIGHTED_CACHE_HIT_RATIO',
                    'weighted average hit rate': 'WEIGHTED_CACHE_HIT_RATIO_SUM',
                    'weight-frequency hit rate': 'AVERAGE_BENEFIT'}
    '''
    for metric_description in combinations:
        print metric_description
        print_cache_hit_rates(file_prefix, '.spickle', goal_tuple=('CACHE_HIT_RATIO', combinations[metric_description]), plot=False)
    '''
    # create plots based on multiple result files
    percentages = [0, 0.1, 0.2, 0.4]
    weights = [1, 5, 10]
    cache_sizes = [1000, 2000, 4000, 8000]

    generate_result_evolution_plots('umass', percentages, weights, cache_sizes, combinations)


if __name__ == "__main__":
    main()