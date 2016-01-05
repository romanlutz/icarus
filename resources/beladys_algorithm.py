import csv
import heapq


MAX_CACHE_SIZE = 500
request_index, cache_hits = 0, 0

traces = []
with open('trace_overview.csv', 'r') as trace_file:
    csv_reader = csv.reader(trace_file)
    for line in csv_reader:
        traces.append(line[0])
        print line[0], ' \t',

occurrences = {}
for trace_path in traces:
    with open(trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)

        # pre-processing
        request_index = 0
        for line in csv_reader:
            request_index += 1
            object = line[2]

            if object in occurrences:
                occurrences[object].append(request_index)
            else:
                occurrences[object] = [request_index]

    with open(trace_path, 'r') as trace:
        csv_reader = csv.reader(trace)
        # actual processing of workload
        request_index = 0
        cache_hits = 0
        cache = {}
        cache_size = 0
        # next_occurrence will be underlying data structure of a max heap
        # heapq library only supports min heaps, so every element will be negated
        next_occurrence = []
        for line in csv_reader:
            request_index += 1
            object = line[2]

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
                    (_, evicted_object) = heapq.heappop(next_occurrence)
                    del cache[evicted_object]

                    if occurrences[object] == []:
                        # newly cached object will not occur in the future
                        cache_size -= 1
                    else:
                        cache[object] = True
                        heapq.heappush(next_occurrence, (-occurrences[object][0], object))

    print float(cache_hits) / float(request_index), ' \t',