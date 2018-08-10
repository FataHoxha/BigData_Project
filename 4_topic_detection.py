import csv

from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f
master="local[4]"
app_name="topic_detection"

print("--> Configuring Spark session")
spark = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()
print("--> Spark session configured")

#READ tweet from csv file

print("--> Importing tweet from csv")
df = spark.read.csv("results_tweet.csv", header=True);

df.show()
df.printSchema()

#extract topic -> partionBy, select max tfidf
print("--> Topic extraction")
w = Window.partitionBy('labels')
res=df.withColumn('max', f.max('max_tfidf').over(w))\
    .where(f.col('max_tfidf') == f.col('max'))\
    .drop('max')
res.show()
print("--> Topic extracted")
#topic extracterd
print ("res--->",res)


keep_col = ['tweet','ngrams_token_words','max_tfidf', 'labels']
topics = res[keep_col]

topics.show()
topics.toPandas().to_csv("topics_tweet.csv", index=False)

print("--> Topic saved to : topics_tweet.csv")


df = spark.read.csv("results_tweet.csv", header=True);

df_user=df.select('user').withColumnRenamed('user','user')

df_labels=df.select('labels').withColumnRenamed('labels','labels')


df_u = df_user.withColumn("id", f.monotonically_increasing_id())
df_l=df_labels.withColumn("id", f.monotonically_increasing_id())
df_all= df_u.join(df_l, "id", "outer").drop("id")
df_all.show()

#print(cleanResult.take(45))

dataReduced = df_all.rdd.reduceByKey(lambda x,y : x + "," + y)
catSplitted = dataReduced.map(lambda (user,labels) : (user, labels.split(",")))
#catCounted = catSplitted.map(lambda (x,y):(x,tuple(set((z,y.count(z)) for z in y))))

catCounted = catSplitted.map(lambda (x,y):(x,list(set(z.encode('utf-8')for z in y ))))
#x.encode('utf-8') for x in tmp
#print(catCounted.take(40))

value=[]
tweet_dict = {}
for result in catCounted.collect():
    print result
    value.append(result)
    #for res in result:
    #    print (res)
user_id=[]
topic=[]
counter=0
for i,j in value:
    user_id.append(i)
    topic.append(j)
    values = zip(user_id, topic)
tweet_dict[counter] = (values)
with open('user_topic.csv', 'w') as out_file:
    writer = csv.writer(out_file)
    writer.writerow(('user_id', 'topic'))
    for key, values in tweet_dict.items():
        writer.writerows(values)