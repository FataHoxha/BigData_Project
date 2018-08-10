from pyspark.sql import SparkSession
import pyfpgrowth
import pandas as pd
master="local[4]"
app_name="topic_pattern"

print("--> Configuring Spark session")
spark = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()
print("--> Spark session configured")

#READ tweet from csv file

print("--> Importing tweet from csv")
df = spark.read.csv("user_topic.csv", header=True);

df.show()
df.printSchema()


items_topics=df.select("topic")

transactions = []
for value in items_topics.collect():
    if value and (len(value) > 0):
        transactions.append(value)
support_threshold = 1
association_patterns = pyfpgrowth.find_frequent_patterns(transactions, support_threshold)
print(association_patterns)
confidence_threshold = 0.5
association_rules = pyfpgrowth.generate_association_rules(association_patterns, confidence_threshold)
print("association rules",association_rules)