from icarus.results.visualize import draw_cache_level_proportions
from icarus.results.output_results import print_cache_hit_rates, print_results_full

def main():
    #draw_cache_level_proportions('plots', 'results', '.spickle')
    #print_results_full('results-weighted-c1000-umass', '.spickle')
    print 'unweighted hit rate'
    print_cache_hit_rates('results-weighted-c1000-umass', '.spickle', goal_tuple=('CACHE_HIT_RATIO', 'MEAN'), plot=False)
    print 'normalized reward'
    print_cache_hit_rates('results-weighted-c1000-umass', '.spickle', goal_tuple=('CACHE_HIT_RATIO', 'WEIGHTED_CACHE_HIT_RATIO'), plot=False)
    print 'weighted average hit rate'
    print_cache_hit_rates('results-weighted-c1000-umass', '.spickle', goal_tuple=('CACHE_HIT_RATIO', 'WEIGHTED_CACHE_HIT_RATIO_SUM'), plot=False)
    print 'weight-frequency hit rate'
    print_cache_hit_rates('results-weighted-c1000-umass', '.spickle', goal_tuple=('CACHE_HIT_RATIO', 'AVERAGE_BENEFIT'), plot=False)

if __name__ == "__main__":
    main()