from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

def sendRecord(df):
    
    from pyspark.ml.classification import RandomForestClassificationModel
    sameModel=RandomForestClassificationModel.load("randomForest.model") 

    df=df.withColumn("amount",df["amount"].cast(FloatType()))
    df=df.withColumn("newbalanceDest",df["newbalanceDest"].cast(FloatType()))
    df=df.withColumn("newbalanceOrig",df["newbalanceOrig"].cast(FloatType()))
    df=df.withColumn("oldbalanceDest",df["oldbalanceDest"].cast(FloatType()))
    df=df.withColumn("oldbalanceOrg",df["oldbalanceOrg"].cast(FloatType()))
    df=df.withColumn("isFlaggedFraud",df["isFlaggedFraud"].cast(IntegerType()))
    df=df.withColumn("step",df["step"].cast(IntegerType()))
    df=df.withColumn("Type",df["Type"].cast(IntegerType()))
    
    assembler=VectorAssembler(
        inputCols=["Type","amount","newbalanceDest","newbalanceOrig","oldbalanceDest","oldbalanceOrg","step"],
        outputCol="features")

    output=assembler.transform(df).select("features")
    
    predictions=sameModel.transform(output)

    pr=predictions.select("prediction")

    pr=pr.rdd

    if(str(pr.collect()==[Row(prediction=1.0)])=="True"):
        print("FRAUD!!!!")
    else:
        print("Not Fraud")

def empty_rdd():
    print("Waiting for input")

if __name__=="__main__":
    
    sc=SparkContext()
    ssc=StreamingContext(sc, 5) #5 second window

    brokers="localhost:9092"
    topic="test"
   
    kvs=KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    
    lines=kvs.map(lambda x: x[1])

    transaction=lines.foreachRDD(lambda rdd:sendRecord(rdd.map(lambda line: line.split(","))\
                                .map(lambda line:Row(step=line[0],
                                                    Type=line[1],
                                                    amount=line[2],
                                                    nameOrig=line[3],
                                                    oldbalanceOrg=line[4],
                                                    newbalanceOrig=line[5],
                                                    nameDest=line[6],
                                                    oldbalanceDest=line[7],
                                                    newbalanceDest=line[8],
                                                    isFlaggedFraud=line[9])).toDF()) if not rdd.isEmpty() else empty_rdd())
  
    ssc.start()
    ssc.awaitTermination()
