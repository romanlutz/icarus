import gzip

def unzip_file(filename):
    with gzip.open(filename, 'rb') as zip_file:
        with open(filename[:-3], 'wt') as unzipped_file:
            unzipped_file.writelines(zip_file)