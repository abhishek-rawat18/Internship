from kafka import KafkaProducer
from time import sleep
from json import dumps

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

#bootstrap_server : sets the host and port the producer should contact the broker
#value serializaer : sets up how the data should be serialized before sending to the broker

for e in range(10):
    data = {'number' : e}
    producer.send('numtest', value=data) #The producer sends numbers 0 to 9
    sleep(5) #take a 5s break to conclude iteration
