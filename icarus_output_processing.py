from icarus.results.visualize import draw_cache_level_proportions
from icarus.results.output_results import print_cache_hit_rates, print_results_full

def main():
    #draw_cache_level_proportions('plots', 'results', '.spickle')
    #print_results_full('results-globo_geant', '.spickle')
    print_cache_hit_rates('results-weighted10', '.spickle',
                          #goal_tuple=('CACHE_HIT_RATIO', 'MEAN'),
                          #goal_tuple=('CACHE_HIT_RATIO', 'WEIGHTED_CACHE_HIT_RATIO_SUM'),
                          #goal_tuple=('CACHE_HIT_RATIO', 'AVERAGE_BENEFIT'),
                          goal_tuple=('CACHE_HIT_RATIO', 'WEIGHTED_CACHE_HIT_RATIO'),
                          plot=False)


if __name__ == "__main__":
    main()