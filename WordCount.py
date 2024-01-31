from pyspark.sql import SparkSession

def wordcount_mapper(word):
    return (word,1)

def wordcount_reducer(a,b):
    return (a+b)

def splitter(line):
    return line.split(" ")

# SparkSession is an entry point into all functionality in Spark
spark =SparkSession.builder.appName("WordCount").master("local").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
# Create RDD from external Data source
read_file=spark.sparkContext.textFile("Input.txt")
# Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
words=read_file.flatMap(splitter)

wordcount_mapped=words.map(wordcount_mapper)

wordcount_reduced=wordcount_mapped.reduceByKey(wordcount_reducer)

temp = wordcount_reduced.collect()

for x in temp:
    print(x)

spark.stop()    