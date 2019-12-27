import json
import sys, getopt
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
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

def produce(data, producer_type):
    time_driven = False
    if producer_type == "time":
        time_driven = True
    producer = KafkaProducer(bootstrap_servers='localhost:9092', 
            value_serializer=lambda m: json.dumps(m).encode('utf-8'))
            #api_version=(2),
            #request_timeout_ms=1000000, api_version_auto_timeout_ms=1000000)
    for item in data:
        print(item)
        print("\n")
        producer.send('iot-test1-topic', value=item)
        print("Here")
        if time_driven:
            time.sleep(1)

#if __name__ == "__main__":
func(sys.argv[1:])
