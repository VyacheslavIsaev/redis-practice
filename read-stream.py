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

def read_redis_stream(stream_name=DEFAULT_STREAM_NAME):
    r = connect_redis()
    while True:
        buff = r.xread({stream_name:'$'}, None, 0)
        print(buff)
        parse_buff(buff)

if __name__ == "__main__":
    read_redis_stream()
