import csv
from collections import defaultdict

def main():
    weights = {}
    with open('12-2015-01-2016.weights', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = int(row[0])
            weight = int(row[1])

            weights[content] = weight


    with open('12-2015-01-2016.trace', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        occurrences = defaultdict(int)

        for row in csv_reader:
            occurrences[int(row[2])] += 1


    total_requests = sum(occurrences.values())
    total_contents = len(occurrences)
    weighted_contents = sum([1 for content in weights if weights[content] > 1])
    weighted_requests = sum([occurrences[content] for content in occurrences if weights[content] > 1])
    print(('%d requests, %d contents' % (total_requests, total_contents)))
    print(('%d weighted requests, %d weighted contents' % (weighted_requests, weighted_contents)))

    single_request_weighted_contents = sum([1 for content in occurrences if weights[content] > 1 and occurrences[content] == 1])
    less_than_ten_requests_weighted_contents = sum([1 for content in occurrences if weights[content] > 1 and occurrences[content] < 10])
    less_than_twenty_requests_weighted_contents = sum([1 for content in occurrences if weights[content] > 1 and occurrences[content] < 20])
    less_than_fifty_requests_weighted_contents = sum([1 for content in occurrences if weights[content] > 1 and occurrences[content] < 50])

    print(('weighted contents with 1 request: %d' % single_request_weighted_contents))
    print(('weighted contents with <10 requests: %d' % less_than_ten_requests_weighted_contents))
    print(('weighted contents with <20 requests: %d' % less_than_twenty_requests_weighted_contents))
    print(('weighted contents with <50 requests: %d' % less_than_fifty_requests_weighted_contents))
    print(('weighted contents with >=50 requests: %d' % (weighted_contents - less_than_fifty_requests_weighted_contents)))

if __name__ == "__main__":
    main()