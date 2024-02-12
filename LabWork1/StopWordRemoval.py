from pyspark.sql import SparkSession

def mapper(word):
    if word not in ['the','that','The','a','is','these']:
        return (word)

def splitter(line):
    return line.split(" ")

spark = SparkSession.builder.appName("WordCount").master("local").getOrCreate()

read_file = spark.sparkContext.textFile("Input.txt")

words = read_file.flatMap(splitter)

ans = words.map(mapper).collect()

for x in ans:
    print(x)

spark.stop()    