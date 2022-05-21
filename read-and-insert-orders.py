
from jsonschema import ValidationError
from redis_utils import *
from stream_utils import *
import datetime
import time

def extract_item(orderDict):
    item = Product(
        StockCode = orderDict['StockCode'],
        Description = orderDict['Description'],
        UnitPrice = orderDict['UnitPrice']
    )

def extract_order(orderDict):
    
        item = extract_item(orderDict)
        order = Order(
            InvoiceNo = orderDict['InvoiceNo'],
            Item = item,
            Quantity = orderDict['Quantity'],
            InvoiceDate = datetime.datetime.strptime(orderDict['InvoiceDate'], '%m/%d/%Y')
            CustomerID = orderDict['CustomerID'],
            Country = orderDict['Country']
        )

def parse_buff(buff):
    for result in buff:
        data = result[1]
        for tuple in data:
            orderDict = tuple[1]
            print(orderDict)
            try:
                order = extract_order(orderDict)
            except ValidationError as e:
                print(e)
                continue
            print(order.key())
            order.save()

def read_redis_stream(stream_name=DEFAULT_STREAM_NAME, records=0):
    r = connect_redis()
    print("Reading from: {}".format(stream_name))
    while True:
        buff = r.xread({stream_name:'$'}, None, 0)
        print(buff)
        parse_buff(buff)

if __name__ == "__main__":

    read_redis_stream()
