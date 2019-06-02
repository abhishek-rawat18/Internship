from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

#auto_offset_reset as earliest means that the consumer starts reading at the latest committed offset.
#enable_auto_commit as True makes sure that the consumer commits its read offset every interval.
#value_deserializer deserializes the data into a common json format, 
#the inverse of what our value serializer was doing in producer.

for message in consumer:
    message = message.value
    print('{}'.format(message))
