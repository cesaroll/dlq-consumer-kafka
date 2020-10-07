from kafka import KafkaConsumer
from pymongo import MongoClient

consumer = KafkaConsumer(
    'dlq-topic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='dlq-consumer-group')

for message in consumer:
  message = message.value.decode('utf-8')
  print('Message: {}'.format(message))


