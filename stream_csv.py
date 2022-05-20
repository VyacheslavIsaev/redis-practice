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
    stream_csv("OnlineRetail.csv", "orders")