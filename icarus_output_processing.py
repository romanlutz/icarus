from icarus.results.visualize import draw_cache_level_proportions
from icarus.results.output_results import print_cache_hit_rates, print_results_full

def main():
    #draw_cache_level_proportions('plots')
    print_results_full()
    print_cache_hit_rates()

if __name__ == "__main__":
    main()