from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'dlq-topic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='dlq-consumer-group')

client = MongoClient('mongodb+srv://test:aAaNuMGzuck1y2YX@cluster0.o6w2h.gcp.mongodb.net/dlq?retryWrites=true&w=majority')
db = client.dlq
collection = db.poisoned_messages

for message in consumer:
  message = message.value.decode('utf-8')
  print('Message: {}'.format(message))
  dict = json.loads(message)
  messageId = collection.insert_one(dict).inserted_id
  print('Inserted Id: {}'.format(messageId))
