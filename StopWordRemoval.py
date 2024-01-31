from pyspark.sql import SparkSession

def wordcount_mapper(word):
    if word not in ['the','that','The','a','is','these']:
        return (word)

def splitter(line):
    return line.split(" ")

spark =SparkSession.builder.appName("WordCount").master("local").getOrCreate()

read_file=spark.sparkContext.textFile("Input.txt")

words=read_file.flatMap(splitter)

wordcount_mapped=words.map(wordcount_mapper)

temp = wordcount_mapped.collect()

for x in temp:
    print(x)

spark.stop()    