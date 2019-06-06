from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import base64

from hdfs import InsecureClient

def sendRecord(tup):
   
    id = tup[0] #the key, i.e. the count in producer program

    value=tup[1] #the base 64 form of the image
    
    original=base64.b64decode(value) #decode the base64 form

    # Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)
    client_hdfs = InsecureClient('http://localhost:50070')

    #Write to a file in the folder xyz in hdfs
    with client_hdfs.write('/xyz/frame%d.jpg' % int(id)) as writer:
        writer.write(original)
        

if __name__=="__main__":
    
    ssc = StreamingContext(sc, 5) # 5 second window

    brokers="localhost:9092"
    topic = "test"
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    
    image_base64=kvs.foreachRDD(lambda rdd:rdd.foreach(sendRecord))
  
    ssc.start()
    ssc.awaitTermination()

    
