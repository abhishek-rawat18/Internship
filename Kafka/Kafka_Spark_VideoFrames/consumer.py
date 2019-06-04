from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import base64

def sendRecord(tup):
   
    id = tup[0] #the key, i.e. the count in producer program

    value=tup[1] #the base 64 form of the image
    
    original=base64.b64decode(value) #decode the base64 form

    #Write to a file
    with open('F:\\Images\\frame%d.jpg' % int(id), 'wb') as f_output:
        f_output.write(original)
        

if __name__=="__main__":
    
    sc = SparkContext(appName="ImagesSave")
    ssc = StreamingContext(sc, 5) # 5 second window

    brokers="localhost:9092"
    topic = "test"
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    
    image_base64=kvs.foreachRDD(lambda rdd:rdd.foreach(sendRecord))
  
    ssc.start()
    ssc.awaitTermination()

    
