# Make sure to install kafka-python library using the command: pip install kafka-python

# Make sure to install JSON library using the command: pip install simplejson

from kafka import KafkaConsumer
import json

# To consume latest messages from beginning without auto-commit offsets
consumer = KafkaConsumer('json-topic',
                         group_id='json=topic-group',
                         bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=False,
                         value_deserializer=lambda m: m.decode('utf-8'),
                         key_deserializer=lambda m: m.decode('utf-8')
                         )

for message in consumer:
    # message value and key are raw bytes -- decode as necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    # The above line prints all the details of the messages with the message value at the end.
    # The line below prints the keys and the messages
    print ("Key=%s: Value=%s:" % (message.key, message.value))
