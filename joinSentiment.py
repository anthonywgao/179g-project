from pyspark.sql import SparkSession

# Constants
database = "yelp_db"
collection = "review_sentiment"
collection2 = "business_collection"
host = "127.0.0.1"
port = "27017"

# Mongo connection URI
connectionString = "mongodb://{0}:{1}/{2}.{3}".format(
    host, port, database, collection)
connectionString2 = "mongodb://{0}:{1}/{2}.{3}".format(
    host, port, database, collection2)

spark = SparkSession.builder \
    .appName("yelp_db") \
    .config("spark.mongodb.input.uri", connectionString) \
    .config("spark.mongodb.output.uri", connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()


# Read from spark session input uri
sentimentDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
sentimentDF = sentimentDF.groupBy("business_id").agg(
    {'predicted_sentiment': 'avg'}).withColumnRenamed('avg(predicted_sentiment)', "predicted_sentiment")

# Read from spark session input uri
businessDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
    "uri", connectionString2).load()
# sentimentDF.show()
businessDF.show()
joinSentiment = sentimentDF.join(businessDF, "business_id")

joinSentiment.write \
    .mode("overwrite") \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", connectionString) \
    .option("collection", "joined_sentiment") \
    .save()

joinSentiment.show()
spark.stop()
