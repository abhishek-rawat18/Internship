from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def sendRecord(tup):
    word   = tup[0]
    amount = tup[1]

    with open('word_file.txt', 'a') as f:
        f.write("%s\n" % word)

if __name__=="__main__":
    
    sc = SparkContext(appName="WordSave")
    ssc = StreamingContext(sc, 10) # 10 second window

    brokers="localhost:9092"
    topic = "test"
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

    counts.foreachRDD(lambda rdd: rdd.foreach(sendRecord)) 
    counts.pprint()
    
    ssc.start()
    ssc.awaitTermination()

    
