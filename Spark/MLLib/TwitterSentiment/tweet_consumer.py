from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
from pyspark.sql import Row
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
import nltk
from nltk.stem.wordnet import WordNetLemmatizer

def sendRecord(df):
    
    hashingTF = HashingTF(inputCol="filteredWords", outputCol="features")
    rescaledData = hashingTF.transform(df)
    
    from pyspark.ml.classification import NaiveBayesModel
    sameModel = NaiveBayesModel.load("TwitterSentimentNB.model") 
    
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    predictions = sameModel.transform(rescaledData)
    
    pr=predictions.select("prediction")
    
    pr=pr.rdd
    
    if((str(pr.collect()==[Row(prediction=1.0)]))=="True"):
        print("Positive tweet")
    else:
        print("Negative tweet")

def transf(tweet):
    tweet=tweet.lower()
    tweet=rg1.sub("",tweet) 
    tweet=rg2.sub("",tweet)
    tweet=tweet.strip().split(" ")
    
    for word in tweet:
        word=lm.lemmatize(word,"v")
        #word=stem.stem(word)
    
    return tweet


if __name__=="__main__":
    
    ssc = StreamingContext(sc, 1) # 1 second window

    brokers="localhost:9092"
    topic = "test"
    
    lm = WordNetLemmatizer()
    remover = StopWordsRemover(inputCol="words", outputCol="filteredWords")
    rg1=re.compile(r'\d+')
    rg2=re.compile(r'[\"\',-.:;&!#$%^*<>=?~_/]')   
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    
    lines = kvs.map(lambda x: x[1])
 

    transaction=lines.foreachRDD(lambda rdd:sendRecord(remover.transform((rdd.map(lambda line:{"words":transf(line[0])})).toDF())) 
                                 if not rdd.isEmpty() else None)
  
    ssc.start()
    ssc.awaitTermination()
