__author__ = 'romanlutz'

import csv

def reformat(filename):
    extension = '_reformatted'
    with open(filename[:-8] + extension + '.trace', 'wb') as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        with open(filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=' ')
            time = 0

            for row in csv_reader:
                content = int(row[0])
                until = int(row[1])

                for j in range(0, until):
                    writer.writerow((time, 0, content+j))

            time += 1

for i in range(1, 15):
    reformat('P%i.lis.txt' % i)
for i in range(1, 4):
    reformat('S%i.lis.txt' % i)
reformat('spc1likeread.lis.txt')
reformat('ConCat.lis.txt')
reformat('DS1.lis.txt')
reformat('MergeP.lis.txt')
reformat('MergeS.lis.txt')
reformat('OLTP.lis.txt')