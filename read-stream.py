import argparse

from redis_utils import *
from stream_utils import *
import datetime
import time

def parse_buff(buff):
    for result in buff:
        data = result[1]
        for tuple in data:
            orderDict = tuple[1]
            print(orderDict)

def read_redis_stream(stream_name=DEFAULT_STREAM_NAME, records=0):
    r = connect_redis()
    print("Reading from: {}".format(stream_name))
    i=0
    while True:
        if records>0 and i >= records:
            break
        buff = r.xread({stream_name:'$'}, None, 0)
        print(buff)
        parse_buff(buff)
        i += 1

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Reads Redis stream.')
    parser.add_argument('-c', dest="channel_name", type=str, default=DEFAULT_STREAM_NAME,
                    help='Name of the channel to stream to.')
    parser.add_argument('-r', dest="records", type=int, default=0,
                    help='Number of records to read.')

    args = parser.parse_args()

    read_redis_stream(args.channel_name, args.records)
