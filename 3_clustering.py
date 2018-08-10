
from pyspark.sql import Row
from pyspark.shell import sqlContext, sc
from pyspark.sql import SparkSession
from pyspark.ml.feature import NGram
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import FloatType
import numpy as np
import pandas as pd

master="local[4]"
app_name="cluster_tweet"

print("--> Configuring Spark session")
spark = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()
print("--> Spark session configured")

#READ tweet from csv file

print("--> Importing tweet from csv")
df = spark.read.csv("clean_tweet.csv", header=True);

df.show()
df.printSchema()

#Tokenize the text in the text column
tokenizer = Tokenizer(inputCol="tweet", outputCol="token_words")
wordsDataFrame = tokenizer.transform(df)

wordsDataFrame.show()

ngram = NGram(n=2, inputCol="token_words", outputCol="ngrams_token_words")
ngramDataFrame = ngram.transform(wordsDataFrame)
ngramDataFrame.select("ngrams_token_words").show(truncate=False)

print("ngramDataFrame ---------------------------->")
print (ngramDataFrame)

hashingTF = HashingTF(inputCol="ngrams_token_words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(ngramDataFrame)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
rescaledData.show()


value=[]
for features_label in rescaledData.select("features").collect():
  for feature in features_label:
      max_value = max(feature)
      value.append(max_value)
print ("test", value)

R = Row('id', 'max_tfidf')

# use enumerate to add the ID column
df_max_tfidf=spark.createDataFrame([R(i, float(x)) for i, x in enumerate(value)])

df_old = rescaledData.withColumn("id", monotonically_increasing_id())

df_all_feature= df_old .join(df_max_tfidf, "id", "outer").drop("id")
df_all_feature.show()


kdata=df_all_feature.select('features').withColumnRenamed('features','features')
# Trains a k-means model.
kmeans = KMeans().setK(5).setSeed(1)
kmodel = kmeans.fit(df_all_feature)
labelsDF = kmodel.transform(df_all_feature).select('prediction').withColumnRenamed('prediction','labels')
labels=labelsDF.collect()
labelsDF.show()



df1 = df_all_feature.withColumn("id", monotonically_increasing_id())
df2=labelsDF.withColumn("id", monotonically_increasing_id())

final_df= df1 .join(df2, "id", "outer").drop("id")
final_df.show()

#save all the data in a file csv
final_df.toPandas().to_csv("res_tweet.csv", header=True)

#save just useful column for topic detection and user profiling in csv file
f=pd.read_csv("res_tweet.csv")
keep_col = ['user','tweet','ngrams_token_words','max_tfidf', 'labels']
new_f = f[keep_col]
new_f.to_csv("results_tweet.csv", index=False)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = kmodel.computeCost(rescaledData)
print("Within Set Sum of Squared Errors = " + str(wssse))


