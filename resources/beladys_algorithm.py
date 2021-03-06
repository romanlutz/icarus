import csv
import heapq
from copy import deepcopy

def beladys_algorithm(max_cache_sizes, traces):
    results = {}
    for s in max_cache_sizes:
        results[s] = []
    warmups = [4*x for x in max_cache_sizes]

    for trace_path in traces:
        with open('resources/' + trace_path, 'r') as trace:
            csv_reader = csv.reader(trace)

            # pre-processing
            original_occurrences = {}
            request_index = 0
            for line in csv_reader:
                request_index += 1
                object = int(line[2])

                if object in original_occurrences:
                    original_occurrences[object].append(request_index)
                else:
                    original_occurrences[object] = [request_index]



        for i, max_cache_size in enumerate(max_cache_sizes):
            warmup = warmups[i]
            with open('resources/' + trace_path, 'r') as trace:
                csv_reader = csv.reader(trace)
                # actual processing of workload
                request_index = 0
                cache_hits = 0
                cache = {}
                cache_size = 0
                occurrences = deepcopy(original_occurrences)
                # next_occurrence will be underlying data structure of a max heap
                # heapq library only supports min heaps, so every element will be negated
                next_occurrence = []
                for line in csv_reader:
                    request_index += 1
                    object = int(line[2])

                    occurrences[object] = occurrences[object][1:]

                    if object in cache:
                        cache_hits += 1
                        next_occurrence.remove((-request_index, object))
                        heapq.heapify(next_occurrence)
                        if occurrences[object] == []:
                            # no future occurrences - remove element from cache
                            cache_size -= 1
                            del cache[object]
                        else:
                            heapq.heappush(next_occurrence, (-occurrences[object][0], object))
                    else:
                        if cache_size < max_cache_size:
                            if occurrences[object] == []:
                                # object won't occur any more
                                pass
                            else:
                                cache[object] = True
                                cache_size += 1
                                heapq.heappush(next_occurrence, (-occurrences[object][0], object))
                        else:
                            # replacement policy: replace the one which is needed the furthest in the future
                            # in case the newly observed element is needed the furthest don't cache it

                            if occurrences[object] == []:
                                # newly cached object will not occur in the future, don't change cache
                                pass
                            else:
                                (furthest_occurrence, furthest_needed_object) = heapq.heappop(next_occurrence)
                                if -furthest_occurrence < occurrences[object][0]:
                                    # new element will occur further in the future, don't cache it
                                    heapq.heappush(next_occurrence, (furthest_occurrence, furthest_needed_object))
                                else:
                                    del cache[furthest_needed_object]
                                    cache[object] = True
                                    heapq.heappush(next_occurrence, (-occurrences[object][0], object))

                    # neglect cache hits from warmup period
                    if request_index == warmup:
                        cache_hits = 0

                # ignore requests from warmup period
                results[max_cache_size].append(str(float(cache_hits) / float(request_index - warmup)))
                print('finished', trace_path, results[max_cache_size][-1])


    for s in max_cache_sizes:
        print('\t'.join(results[s]))