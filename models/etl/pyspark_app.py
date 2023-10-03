
# Import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
   .appName("My App") \
   .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1, 100))

# Calculate the sum and collect the result to the driver
sum_result = rdd.sum()
print('\n************************************\n')
print("THE SUM IS HERE: ", sum_result)
print('\n************************************\n')

# Stop the SparkSession
spark.stop()
