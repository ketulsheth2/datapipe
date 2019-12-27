import sys
import json
from confluent_kafka import Consumer, KafkaError
from influxdb import InfluxDBClient

client = InfluxDBClient(host='influxdb', port=8086, database='logdb')
client.create_database('logdb')

c = Consumer({
    'bootstrap.servers': 'broker:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
c.subscribe(['LOG_TABLE_DEVICE_COUNT'])

try:
    while True:
        print('Waiting for message..')
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
    
        #print('Received message: {}'.format(msg.value().decode('utf-8')))
        json_body = [
        {
            "measurement": "device-count", #this table will be automatically created
            "tags": {
            },
            "fields":json.loads(msg.value())
        }
        ]
        print(json_body)
        client.write_points(json_body)

except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

finally:
        # Close down consumer to commit final offsets.
        c.close()
