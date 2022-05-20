import csv

from redis_utils import *

FILE_NAME = "data.csv"

def stream_csv(file_name, stream_name="stream", encoding='utf-8-sig'):    
    with open(file_name, encoding=encoding) as csvf:
        csvReader = csv.DictReader(csvf)
        r = connect_redis()
        for row in csvReader:
            r.xadd(stream_name, row)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('file_name', metavar='N', type=string, nargs='+',
                    help='Name of the file to read.')
    parser.add_argument('stream_name', metavar='N', type=string, nargs='+',
                    help='Name of the channel to stream to.')
    parser.add_argument('encoding_name', metavar='N', type=string, nargs='+',
                    help='File encoding')

    args = parser.parse_args()

    stream_csv("OnlineRetail.csv", "orders")