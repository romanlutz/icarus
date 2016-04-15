import csv

def generate_uniform_weights_file(trace_file_name, weights_file_name):
    contents = {}
    with open(trace_file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            content = int(row[2])
            weight = 1
            contents[content] = weight

    with open(weights_file_name, 'w') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_NONE)
        for content in contents:
            writer.writerow((content, contents[content]))