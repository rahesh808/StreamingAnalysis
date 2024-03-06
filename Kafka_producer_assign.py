# Make sure to install kafka-python library using the command: pip install kafka-python

# Make sure to install JSON library using the command: pip install simplejson

from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import json

# Create KafkaProducer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Publish each line from the given file to the topic
# If the topic specified below is absent, then it will be created.
# If it is present then it will be used for pubishing the data

with open("stock_stream_data.csv", "r") as file:
    data = file.readlines()
    for line in data:
        mykey, myvalue=line.split(',',1)
        producer.send('json-topic', key=mykey.encode('utf-8'), value=myvalue.strip().encode('utf-8'))
        time.sleep(2)
