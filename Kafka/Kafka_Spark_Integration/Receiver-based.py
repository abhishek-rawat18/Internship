from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
    ssc = StreamingContext(sc, 5) # 5 second window

    broker="localhost:2181"
    topic = "test"
    kvs = KafkaUtils.createStream(ssc,broker,"streaming-consumer",{topic:1}) 
    #Receiver Based Approach

    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    #Count the number of words
    
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

