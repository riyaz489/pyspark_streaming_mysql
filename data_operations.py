from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as f
from pyspark.sql import Window
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:/home/nineleaps/spark-3.0.0-preview2-bin-hadoop2.7/jars/' \
                                    'mysql-connector-java-8.0.19.jar pyspark-shell'
url = "jdbc:mysql://localhost:3306/twitter"
driver = "com.mysql.jdbc.Driver"
properties = {
    "user": "root",
    "password": "",
    "driver": driver
}

sc = SparkContext(appName="TestPySparkJDBC", master="local")
sqlContext = SQLContext(sc)
df = sqlContext.read.format('jdbc').\
    options(driver=driver, url=url, dbtable='hashtags').option("user", "root").option("password", "").load()

w = Window.partitionBy('place_id', 'hashtag')
hashtag_count = df.\
    withColumn('hashtag_count', f.count('id').over(w)).\
    sort(f.asc('place'), f.desc('hashtag_count'))


# query 1 : Most popular tags for each location with their count values
w = Window.partitionBy('place')
most_popular_tags = hashtag_count.\
    withColumn('max_count', f.max('hashtag_count').over(w)).\
    where(f.col('hashtag_count') == f.col('max_count'))

location_wise_popular_tags = most_popular_tags.\
    select('place', 'hashtag', 'hashtag_count').\
    distinct().\
    sort(f.asc('place'), f.asc('hashtag'), f.desc('hashtag_count'))
# writing data to db
location_wise_popular_tags.write.jdbc(url=url, table="popular_tags", mode="overwrite", properties=properties)
location_wise_popular_tags.show(40)


# query 2 :for most popular #tags, find most popular #tags used in combination with.
t3 = df.alias('df1').join(most_popular_tags.alias('df2'), ((df.tweet_id == most_popular_tags.tweet_id) &
                                                           (df.hashtag != most_popular_tags.hashtag)), how='inner')
t4 = t3.groupby('df2.hashtag', 'df1.hashtag').agg(f.count("df1.tweet_id").alias("count"))
w = Window.partitionBy('df2.hashtag')
t5 = t4.withColumn('max', f.max('count').over(w)).where(f.col('count') == f.col('max')).selectExpr('df2.hashtag as hashtag', 'df1.hashtag as other_combination_tag', 'count as other_tag_count')
# writing data to db
t5.write.jdbc(url=url, table="popular_tags_popular_combination", mode="overwrite", properties=properties)
t5.show()


# query 3 : find out the per hour frequency of popular #tag tweet for each location.
w = Window.partitionBy("place", "hashtag", "date", "hour")
per_hour_frequency = most_popular_tags.\
    withColumn("date", f.to_date(f.col("created_at"))).\
    withColumn("hour", f.hour(f.col("created_at"))).\
    withColumn("tag_count", f.count('id').over(w)).\
    select('place', 'date', 'hour', 'hashtag', 'tag_count').\
    distinct().\
    sort(f.asc('place'), f.asc('hashtag'), f.asc('date'), f.asc('hour'), f.desc('tag_count'))
# storing data to db
per_hour_frequency.write.jdbc(url=url, table="tags_frequency", mode="overwrite", properties=properties)
per_hour_frequency.show()

