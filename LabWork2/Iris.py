from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# spark session
spark =SparkSession.builder.appName("Iris").master("local").getOrCreate()
spark.sparkContext.setLogLevel('OFF')

# load the dataframe
df = spark.read.csv('LabWork2/iris.csv', header=True, inferSchema=True)
# print the schema
# df.printSchema()
# # print the dataframe
# df.show()

# 1. What is the count of each “variety”?
print('1. What is the count of each “variety” ?')
df.groupBy("variety").count().show()

# 2. Find the average of "sepal.length","sepal.width","petal.length","petal.width" based on the variety
print('\n2. Find the average of "sepal.length", "sepal.width", "petal.length", "petal.width" based on the variety')
avg_values = df.groupBy("variety").avg("`sepal.length`", "`sepal.width`", "`petal.length`", "`petal.width`")
avg_values.show()

# 3. Find the difference between the average of the above mentioned based on variety.
# Eg: y is the difference between mean sepal.length between ,”Setosa” and “Versicolor” 
#        You are expected to find the value of y for everything
print("\n3. Find the difference between the average of the above mentioned based on variety.")
setosa_avg = avg_values.filter(avg_values['variety'] == 'Setosa').first()
versicolor_avg = avg_values.filter(avg_values['variety'] == 'Versicolor').first()
virginica_avg = avg_values.filter(avg_values['variety'] == 'Virginica').first()
diff_setosa_versicolor = {col: setosa_avg[col] - versicolor_avg[col] for col in setosa_avg.asDict().keys() if col != 'variety'}
diff_setosa_virginica = {col: setosa_avg[col] - virginica_avg[col] for col in setosa_avg.asDict().keys() if col != 'variety'}
diff_versicolor_virginica = {col: versicolor_avg[col] - virginica_avg[col] for col in versicolor_avg.asDict().keys() if col != 'variety'}
print("Difference between Setosa and Versicolor:")
print(diff_setosa_versicolor)
print("\nDifference between Setosa and Virginica:")
print(diff_setosa_virginica)
print("\nDifference between Versicolor and Virginica:")
print(diff_versicolor_virginica)

# 4. Find the outliers for each variety.
# Process:   
# - Find the mean, then standard deviation
# - If some value is outside the mean ± standard deviation consider it as outlier
print("\n4. Find the outliers for each variety.")
def find_outliers(df, col_name, variety):
    mean = df.filter(df["variety"] == variety).agg({col_name: "avg"}).collect()[0][0]
    sd = df.filter(df["variety"] == variety).agg({col_name: "stddev"}).collect()[0][0]
    lower_bound = mean - sd
    upper_bound = mean + sd
    outliers = df.filter((df["variety"] == variety) & ((df[col_name] < lower_bound) | (df[col_name] > upper_bound)))
    return outliers

columns = ["`sepal.length`", "`sepal.width`", "`petal.length`", "`petal.width`"]
varieties = ["Setosa",'Versicolor','Virginica']

for col_name in columns:
    for variety in varieties:
        outliers = find_outliers(df, col_name, variety)
        print(f"Outliers for {col_name} in {variety}:")
        outliers.show()
spark.stop()