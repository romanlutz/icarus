import csv
import heapq
from copy import deepcopy


MAX_CACHE_SIZES = [1000]#[100, 500, 1000]
results = {}
for s in MAX_CACHE_SIZES:
    results[s] = []
request_index, cache_hits = 0, 0

traces = []
with open('trace_overview.csv', 'r') as trace_file:
    csv_reader = csv.reader(trace_file)
    for line in csv_reader:
        traces.append(line[0])
        print line[0], ' \t',

print ''


for trace_path in traces:
    with open(trace_path, 'r') as trace:
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



    for MAX_CACHE_SIZE in MAX_CACHE_SIZES:
        with open(trace_path, 'r') as trace:
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
                    if cache_size < MAX_CACHE_SIZE:
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

            results[MAX_CACHE_SIZE].append(str(float(cache_hits) / float(request_index)))
            print 'finished', trace_path, results[MAX_CACHE_SIZE][-1]


for s in MAX_CACHE_SIZES:
    print '\t'.join(results[s])