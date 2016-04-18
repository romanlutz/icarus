from icarus.results.visualize import draw_cache_level_proportions
from icarus.results.output_results import print_cache_hit_rates, print_results_full
import time

def main():
    #draw_cache_level_proportions('plots', '.spickle')
    #print_results_full('.spickle')
    print_cache_hit_rates('.spickle', trace=False)


if __name__ == "__main__":
    main()