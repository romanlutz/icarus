import csv

def main():
    weighted_contents = set([])
    with open('12-2015-01-2016.weights', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for row in csv_reader:
            content = int(row[0])
            weight = int(row[1])

            if weight == 2:
                weighted_contents.add(content)


    with open('12-2015-01-2016.trace', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        weighted = 0
        total = 0

        for row in csv_reader:
            total += 1

            if int(row[2]) in weighted_contents:
                weighted += 1

    print('%f' % (float(weighted)/float(total)))



if __name__ == "__main__":
    main()