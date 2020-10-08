import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from kafka import KafkaProducer

schema = avro.schema.parse(open("dlq.avsc", "rb").read())

writer = DataFileWriter(open("dlq.avro", "wb"), DatumWriter(), schema)
writer.append({"payload": "Some payload message", "consume_time": 123456})
# writer.append({"payload": "Another payload message", "consume_time": 891011})
writer.close()

reader = DataFileReader(open("dlq.avro", "rb"), DatumReader())

for dlq in reader:
    print(dlq)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('dlq-topic', value=dlq)
    producer.flush(30)
reader.close()


# kafka-avro-console-producer \
# --broker-list localhost:9092 --topic dlq-topic \
# --property value.schema='{"type":"record","name":"dlq","fields":[{"name":"payload","type":"string"},{"name": "consume_time",  "type": "int"}]}' \
#  < avrofile.avro
