import argparse
import csv

from redis_utils import *
from stream_utils import *

FILE_NAME = "data.csv"

def stream_csv(file_name, stream_name=DEFAULT_STREAM_NAME, encoding='utf-8-sig', records=0, start=0):
    print("Sending {} records from {}, starting at {} to {}".format(records, file_name, start, stream_name))
    with open(file_name, encoding=encoding) as csvf:
        csvReader = csv.DictReader(csvf)
        r = connect_redis()
        i=0
        sent_records=0
        for row in csvReader:
            if records>0 and sent_records >= records:
                break
            if i < start:
                i += 1
                continue            
            r.xadd(stream_name, row)
            sent_records += 1            
            print ("Sending record: {} Sent: {}".format(i, sent_records))
            i += 1

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Stream CSV file to Redis stream.')
    parser.add_argument('file_name', type=str,
                    help='Name of the file to read.')
    parser.add_argument('-c', type=str, dest='channel_name', default=DEFAULT_STREAM_NAME,
                    help='Name of the channel to stream to.')
    parser.add_argument('-e', type=str, dest='file_encoding', default='utf-8-sig',
                    help='File encoding')
    parser.add_argument('--records', dest='records', type=int, default=0,
                    help='File encoding')
    parser.add_argument('--start',   dest='start', type=int, default=0,
                    help='File encoding')

    args = parser.parse_args()

    stream_csv(args.file_name, args.channel_name, encoding=args.file_encoding, records=args.records, start=args.start)

