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


    for i in range(9):
        with open('12-2015-01-2016-%d.trace' % i, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            weighted = 0
            total = 0

            for row in csv_reader:
                total += 1

                if int(row[2]) in weighted_contents:
                    weighted += 1

        print('%d: %f' % (i, float(weighted)/float(total)))



if __name__ == "__main__":
    main()