These programs demonstate how one can integrate kafka along with spark streaming.

2 approaches have been followed:-

1) Receiver based approach : This approach uses a Receiver to receive the data. The Receiver is implemented using the Kafka high-level consumer API. As with all receivers, the data received from Kafka through a Receiver is stored in Spark executors, and then jobs launched by Spark Streaming processes the data.

2) Direct approach (Non receiver based) : This approach periodically queries Kafka for the latest offsets in each topic+partition, and accordingly defines the offset ranges to process in each batch. When the jobs to process the data are launched, Kafka’s simple consumer API is used to read the defined ranges of offsets from Kafka

To run these programs, you first need to start zookeeper and kafka server. Then run the kafka producer followed by this program.

The program SaveWords.py is just a very simple example of how one can perform operations on the individual RDDs of a DStream. Here I have just saved the words in a file. We can do a lot more than that. 
