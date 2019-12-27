import json
import sys, getopt
import time
from confluent_kafka import Producer
import logging
logging.basicConfig(level=logging.DEBUG)

#Get Command Line Arguments
def func(argv):
    input_file = ''
    producer_type = ''
    try:
        opts, args = getopt.getopt(argv,"hi:t:",["ifile=", "type"])
    except getopt.GetoptError:
        print 'parser.py -i <path to json inputfile> -t <time/dump>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'parser.py -i <path to json inputfile> -t <time/dump>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-t", "--type"):
            producer_type = arg
    data = read_json(input_file)
    produce(data, producer_type)

def read_json(file):
    with open(file, 'r') as f:
        data = json.load(f)
    return data

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                (msg.topic(), msg.partition(), msg.offset()))

def serialize(item):
    return json.dumps(item, default=lambda o: o.__dict__)

def produce(data, producer_type):
    time_driven = False
    if producer_type == "time":
        time_driven = True
    conf = {'bootstrap.servers': 'localhost:29094'}
    p = Producer(**conf)
    
    for item in data:
        print(item)
        print("\n")
        p.produce('log-input', value=serialize(item), callback=delivery_callback)
        if time_driven:
            time.sleep(5)

if __name__ == "__main__":
    func(sys.argv[1:])
