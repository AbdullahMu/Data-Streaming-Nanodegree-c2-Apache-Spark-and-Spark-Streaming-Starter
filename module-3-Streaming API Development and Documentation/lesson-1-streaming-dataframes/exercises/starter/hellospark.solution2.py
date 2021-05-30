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
numAs = 0
numBs = 0

def countA(row):
    global numAs
    numAs += row.value.count('a')
    print('***Total A count', numAs)

def countB(row):
    global numBs
    numBs += row.value.count('b')
    print('***Total B count', numBs)

# TO-DO: call appropriate functions to filter the data containing letters 'a'
# and 'b', and then count the rows that were found
logData.forEach(countA)
logData.forEach(countB)

# TO-DO: stop the spark application
spark.stop()
