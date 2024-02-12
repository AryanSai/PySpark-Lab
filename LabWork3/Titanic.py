from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,col,avg,when,floor, count,sum

# spark session
spark =SparkSession.builder.appName("Titanic").master("local").getOrCreate()
spark.sparkContext.setLogLevel('OFF')

# load the dataframe
df = spark.read.csv('LabWork3/Titanic-Dataset.csv', header=True, inferSchema=True)
# print the schema
# df.printSchema()
# # print the dataframe
# df.show()
# df.describe('Fare').show()

# 1. What is the mean of ticket fare?
print('\n1. What is the mean of ticket fare?')
mean_fare = df.select(mean(df['Fare'])).collect()[0][0]
print("Mean of Ticket Fare:", mean_fare,"\n")

# 2. Provide the six point summary of age based on the survivability.
print('\n2. Provide the six point summary of age based on the survivability.')
survived_summary = df.filter(col('survived') == 1).select('age').summary("min", "25%", "50%", "75%", "max", "count")
not_survived_summary = df.filter(col('survived') == 0).select('age').summary("min", "25%", "50%", "75%", "max", "count")
print("Six-point summary of age for survived individuals:")
survived_summary.show()
print("Six-point summary of age for not survived individuals:")
not_survived_summary.show()

# 3. What is the rate of survival of passengers, if they have siblings vs not having siblings.
print('\n3. What is the rate of survival of passengers, if they have siblings vs not having siblings.')
total_siblings_count = df.filter(col('SibSp') > 0).count()
total_no_siblings_count = df.filter(col('SibSp') == 0).count()
siblings_survived_count = df.filter((col('SibSp') > 0) & (col('survived') == 1)).count()
no_siblings_survived_count = df.filter((col('SibSp') == 0) & (col('survived') == 1)).count()
print("Survival rate of passengers with siblings:", siblings_survived_count / total_siblings_count)
print("Survival rate of passengers without siblings:", no_siblings_survived_count / total_no_siblings_count)

# 4. What is the probability of survival based on the gender.
print('\n4. What is the probability of survival based on the gender.')
total_survived = df.filter(col('survived') == 1).count()
male_survived_count = df.filter((col('sex') == 'male') & (col('survived') == 1)).count()
female_survived_count = df.filter((col('sex') == 'female') & (col('survived') == 1)).count()
print("Survival probability of males:", male_survived_count / total_survived)
print("Survival probability of females:", female_survived_count / total_survived)

# 5. Based on the age, which group managed to survive more relativly?   
print('\n5. Based on the age, which group managed to survive more relativly ?') 
# - You can take 10 years per age group. Say 0 - 10; 11-20 so on
# - Do you think that females outlived male all the age group. Enumerate your learnings.
new_df = df.withColumn("age_group", floor(col("age") / 10) * 10)
survival_rates = new_df.groupBy("age_group").agg(
    (sum(when(col("survived") == 1, 1).otherwise(0)) / count("*")).alias("survival_rate")
).orderBy("age_group")
max_group = survival_rates.orderBy(col("survival_rate").desc()).first()
print("Answer:", max_group[0], " age group with survival rate of ", max_group[1])

#6. What is the average survival rate based on the Embarked City?
print('\n6. What is the average survival rate based on the Embarked City?')
survival_rates = df.groupBy('Embarked').agg(avg(when(col('survived') == 1, 1).otherwise(0)).alias('survival_rate'))
survival_rates.show()

# 7."A passenger from first class is more likely to sucummb then the passenger from 3rd Class"
#     Prove or disprove the hypothesis with the data and 95% of confidence.
print("\n7.A passenger from first class is more likely to sucummb than the passenger from 3rd Class Prove or disprove the hypothesis with the data and 95 percent of confidence.")


# 8.Which passenger group has the highest survival rate based on the age 
# group, gender, class and boarding city? Find the least survival group as well.
print("\n8.Which passenger group has the highest survival rate based on the age group, gender, class and boarding city? Find the least survival group as well.")
survival_rates = df.groupBy("age", "sex", "Pclass", "Embarked").agg(
    (sum(when(col("survived") == 1, 1).otherwise(0)) / count("*")).alias("survival_rate")
)
highest = survival_rates.orderBy(col("survival_rate").desc()).first()
least = survival_rates.orderBy(col("survival_rate")).first()
print("Passenger group with the highest survival rate:", highest)
print("\nPassenger group with the least survival rate:", least)
spark.stop()