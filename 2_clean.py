from pyspark.shell import sqlContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
import tweet_library as tl
import pandas as pd
master="local[4]"
app_name="clean_tweet"

print("--> Configuring Spark session")
spark = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()
print("--> Spark session configured")

#READ tweet from csv file

print("--> Importing tweet from csv")
df = spark.read.csv("tweet.csv", header=True);

df.show()
df.printSchema()
print("-->Cleaning Tweets")
def cleanTweet(text):
    #row and basic operations on tweet
    text=tl.remove_html(text)
    text=tl.remove_url(text)
    text=tl.remove_punctation(text)
    text=tl.remove_digits(text)
    text=tl.lower_case(text)
    #text=tl.remove_non_english(text)
    text=tl.remove_empty_space(text)
    text=tl.text_normalization(text)
    text = tl.remove_empty_space(text)
    #lemmatizer
    #text=tl.lemmatization(text)
    #text=tl.stop_words(text)
    #text=tl.remove_short_word(text)
    #stop word
    #word<2
    #

    #lemmatization step
    return text

    #removing unuseful word -> stop words



def cleanUserId(user_id):
    user_id=tl.remove_user_link(user_id)
    return user_id

print("-->Cleaning Text of Tweets")
udf_clean_text=udf(lambda text: cleanTweet(text), StringType())
print("-->Cleaning User Id")
udf_clean_user_id=udf(lambda user_id: cleanUserId(user_id), StringType())

cleaned_df_user = df.withColumn(("user"), udf_clean_user_id("user_id").alias("user"))

cleaned_df_text = df.withColumn(("tweet"), udf_clean_text("text").alias("tweet"))

cleaned_df1 = cleaned_df_text.withColumn("id", monotonically_increasing_id())
cleaned_df2=cleaned_df_user.withColumn("id", monotonically_increasing_id())

new_df1= cleaned_df1.join(cleaned_df2, "id", "outer").drop("id")

old_df=new_df1.select("user","tweet")
new_df = old_df.filter("tweet != ''")
new_df.show()
print("--> Data cleaned and merged")
#generates multiple csv with dataframes
#new_df.write.csv("new3.csv", sep=',', header = True)

#With pandas we generate
keep_col = ['user','tweet']
new_f = new_df[keep_col]
new_f.toPandas().to_csv("clean_tweet.csv", index=False)

#new_df.toPandas().to_csv("clean_tweet.csv", header=True)
print("--> Data saved in a new csv file")
