from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt
logFile = "/home/workspace/Test.txt"  # Should be some file on your system

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file
logData = spark.read.text(logFile).cache()

# TO-DO: Define a python function that accepts row as in an input, and
# increments the total number of times the letter 'a' has been encountered (including in this row)
numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

# TO-DO: print the count for letter 'a' and letter 'b'
print("*******")
print("*******")
print("*****Lines with d: %i, lines with s: %i" % (numAs, numBs))
print("*******")
print("*******")

# TO-DO: stop the spark application
spark.stop()
