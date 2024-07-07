# Tutorial: Tweet Collection Using Kafka and Spark Streaming 

This tutorial explains how to collect tweets in real-time using a Kafka and Spark streaming.

It requires two python files that run the following processes:
- [`kafka_producer_twitter.py`](streaming/kafka_producer_twitter.py)
    - Using Tweepy and a Kafka producer Tweets are received in real-time and send them to a broker, under a topic.
- [`tweet_collector.py`](streaming/tweet_collector.py)
    - Use a Kafka consumer to collect the Tweets and place them in a Spark DStream of RDDs.
    - Using Spark processing, transform the RDDs using Spark MapReduce functions into double-tsv format, and save as text-files.

#### How to Run the Files

1. Create a Dataproc cluster:
    ```
   gcloud dataproc clusters create tweeter --project st446-267513 \
                                         --subnet default \
                                         --master-machine-type n1-standard-4 \
                                         --master-boot-disk-size 500 \
                                         --num-workers 0 \
                                         --worker-machine-type n1-standard-4 \
                                         --worker-boot-disk-size 500 \
                                         --image-version 1.3-deb9 \
                                         --initialization-actions 'gs://dataproc-initialization-actions/jupyter/jupyter.sh'
                                                                 ,'gs://dataproc-initialization-actions/python/pip-install.sh'
                                                                 ,'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh'
                                                                 ,'gs://dataproc-initialization-actions/kafka/kafka.sh' \
                                         --metadata 'PIP_PACKAGES=sklearn nltk pandas graphframes pyspark kafka-python tweepy'
    ```                                     
2. Upload python file into bucket and then transfer them from the bucket to the cluster
    ```
    gsutil cp "...\kafka_twitter_producer.py" gs://----bucket
    gsutil cp "...\pyspark_mean_variance.py" gs://----bucket

    gsutil cp gs://----bucket/kafka_twitter_producer.py .
    gsutil cp gs://----bucket/pyspark_mean_variance.py .
    ```
3. Append the Kafka streaming jar file `spark.jars.packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0` to Spark by appending the `spark-defaults.config` file:
    ```angular2
    cd $SPARK_HOME
    sudo nano $SPARK_HOME/conf/spark-defaults.conf
    ```
4. Start the Kafka Server:
    ```
    cd /usr/lib/kafka
    sudo -s
    bin/kafka-server-start.sh config/server.properties &
    ```
5. Start the Kafka producer, specifying the topic choice and filter words:
    ```angular2
    python kafka_twitter_producer.py --topic \
                                     --filter
    ```
    `topic` : the name of the Kafka topic received Tweets will be assigned to

    `filter` : list of filter words, only Tweets that contain these words will be collected

6. Run the Tweet collector PySpark file, specifying arguments. 
    1. starts the Kafka consumer
    2. loads the tweets into a Spark DStream of RDDs
    3. transforms the Tweet JSON format into TSV format through RDD transformations
    4. saves each RDD in the stream as a set of text-files (1 for each partition in the RDD)

    ```angular2
    cd $SPARK_HOME
    unset PYSPARK_DRIVER_PYTHON
    bin/spark-submit ~/pyspark_mean_variance.py --interval_length \
                                                --num_partitions \
                                                --path \
                                                --prefix \
                                                --topic \
                                                --graph \
                                                --graph_type
   
    ```
    `interval_length`: length of time interval for each batch of tweets in the stream
    
    `num_partitions`: the number of partitions to coalesce the RDD to before saving as a textfile
    
    `path` : path to where text-files are saved
    
    `prefix` : prefix to the text-file name
    
    `topic` : name of Kafka topic for the consumer to connect to
    
    `graph` : True if output is a graph type, False if a tweet row type
    
    `graph_type` : tweeter_graph or hashtag_graph

