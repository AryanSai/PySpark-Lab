from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# spark session
spark =SparkSession.builder.appName("WordCount").master("local").getOrCreate()
spark.sparkContext.setLogLevel('OFF')

# load the dataframe
df = spark.read.csv('LabWork3/Titanic-Dataset.csv', header=True, inferSchema=True)
# print the schema
# df.printSchema()
# # print the dataframe
# df.show()
import scipy.stats as stats

first_class_survived = df.filter((col('Pclass') == 1) & (col('survived') == 1)).count()
first_class_total = df.filter(col('Pclass') == 1).count()
third_class_survived = df.filter((col('Pclass') == 3) & (col('survived') == 1)).count()
third_class_total = df.filter(col('Pclass') == 3).count()

frequencies = [[first_class_survived, first_class_total - first_class_survived],
                        [third_class_survived, third_class_total - third_class_survived]]

# Perform chi-square test manually
chi2_stat, p_value, _, _ = stats.chi2_contingency(frequencies)

# Print the chi-square statistic and p-value
print("Chi-square statistic:", chi2_stat)
print("P-value:", p_value)

# Compare the p-value with the significance level (0.05) to make a conclusion
if p_value < 0.05:
    print("With 95% confidence, we reject the null hypothesis.")
    print("There is a significant difference in survival rates between passengers from the first class and passengers from the third class.")
else:
    print("With 95% confidence, we fail to reject the null hypothesis.")
    print("There is no significant difference in survival rates between passengers from the first class and passengers from the third class.")
