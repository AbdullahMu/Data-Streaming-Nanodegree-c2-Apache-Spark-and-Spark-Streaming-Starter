from pyspark.sql import SparkSession

# create a Spak Session, and name the app something relevant
# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("fuel-level").getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# read a stream from the kafka topic 'balance-updates', with the bootstrap server kafka:19092, reading from the earliest message
kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","fuel-level")                  \
    .option("startingOffsets","earliest")\
    .load()

# cast the key and value columns as strings and select them using a select expression function
# this is necessary for Kafka Data Frame to be readable, into a single column  value
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this takes the stream and "sinks" it to the console as it is updated one at a time like this:
# +--------------------+-----+
# |                 Key|Value|
# +--------------------+-----+
# |1593939359          |13...|
# +--------------------+-----+

# write the dataframe to the console, and keep running indefinitely
kafkaStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
