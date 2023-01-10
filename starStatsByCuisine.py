from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

# Constants
database = "yelp_db"
collection = "business_collection"
host = "127.0.0.1"
port = "27017"

# Mongo connection URI
connectionString = "mongodb://{0}:{1}/{2}.{3}".format(
    host, port, database, collection)

spark = SparkSession.builder \
    .appName("yelp_db") \
    .config("spark.mongodb.input.uri", connectionString) \
    .config("spark.mongodb.output.uri", connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()


def pickFirstCat(column):
    arr = column.split(", ")
    for x in arr:
        if (x == "Thai") | (x == "Chinese") | (x == "Japanese") | (x == "Korean") | (x == "Indian") | (x == "American") | (x == "Caribbean") | (x == "Italian") | (x == "Mediterranean") | (x == "Mexican") | (x == "Vietnamese") | (x == "Cajun") | (x == "Greek"):
            return x
    return "Other"


udf_pfc = udf(lambda column: pickFirstCat(column), StringType())


# Read from spark session input uri
reviewDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# reviewDF.printSchema()

# see if you can do something like this: https://stackoverflow.com/questions/66510877/how-to-check-if-element-in-array-contains-any-values-from-a-list-python
minRatingByCuisine = reviewDF
minRatingByCuisine.filter(col("categories").contains("Restaurant") & (col("categories").contains("Chinese") | col("categories").contains("Thai") | col("categories").contains("Japanese") | col("categories").contains("Korean") | col("categories").contains("Indian") | col("categories").contains("American") | col("categories").contains("Caribbean") | col(
    "categories").contains("Italian") | col("categories").contains("Mediterranean") | col("categories").contains("Mexican") | col("categories").contains("Vietnamese") | col("categories").contains("Cajun") | col("categories").contains("Greek"))).withColumn("categories", udf_pfc(col("categories"))).groupBy("categories").agg({'stars': 'min', }).show()


minRatingByCuisine.write \
.mode("overwrite") \
.format("com.mongodb.spark.sql.DefaultSource") \
.option("uri", connectionString) \
.option("collection", "avg_rating_cuisine") \
.save()

minRatingByCuisine.show()

maxRatingByCuisine = reviewDF
maxRatingByCuisine.filter(col("categories").contains("Restaurant") & (col("categories").contains("Chinese") | col("categories").contains("Thai") | col("categories").contains("Japanese") | col("categories").contains("Korean") | col("categories").contains("Indian") | col("categories").contains("American") | col("categories").contains("Caribbean") | col(
    "categories").contains("Italian") | col("categories").contains("Mediterranean") | col("categories").contains("Mexican") | col("categories").contains("Vietnamese") | col("categories").contains("Cajun") | col("categories").contains("Greek"))).withColumn("categories", udf_pfc(col("categories"))).groupBy("categories").agg({'stars': 'max', }).show()


maxRatingByCuisine.write \
.mode("overwrite") \
.format("com.mongodb.spark.sql.DefaultSource") \
.option("uri", connectionString) \
.option("collection", "avg_rating_cuisine") \
.save()

maxRatingByCuisine.show()

# medianRatingByCuisine = reviewDF
# medianRatingByCuisine.filter(col("categories").contains("Restaurant") & (col("categories").contains("Chinese") | col("categories").contains("Thai") | col("categories").contains("Japanese") | col("categories").contains("Korean") | col("categories").contains("Indian") | col("categories").contains("American") | col("categories").contains("Caribbean") | col(
#     "categories").contains("Italian") | col("categories").contains("Mediterranean") | col("categories").contains("Mexican") | col("categories").contains("Vietnamese") | col("categories").contains("Cajun") | col("categories").contains("Greek"))).withColumn("categories", udf_pfc(col("categories"))).groupBy("categories").agg({'stars': 'median', }).show()

# medianRatingByCuisine.write \
# .mode("overwrite") \
# .format("com.mongodb.spark.sql.DefaultSource") \
# .option("uri", connectionString) \
# .option("collection", "median_rating_cuisine") \
# .save()

# medianRatingByCuisine.show()

# stdevRatingByCuisine = reviewDF
# stdevRatingByCuisine.filter(col("categories").contains("Restaurant") & (col("categories").contains("Chinese") | col("categories").contains("Thai") | col("categories").contains("Japanese") | col("categories").contains("Korean") | col("categories").contains("Indian") | col("categories").contains("American") | col("categories").contains("Caribbean") | col(
#     "categories").contains("Italian") | col("categories").contains("Mediterranean") | col("categories").contains("Mexican") | col("categories").contains("Vietnamese") | col("categories").contains("Cajun") | col("categories").contains("Greek"))).withColumn("categories", udf_pfc(col("categories"))).groupBy("categories").agg({'stars': 'stdev', }).show()

# stdevRatingByCuisine.write \
# .mode("overwrite") \
# .format("com.mongodb.spark.sql.DefaultSource") \
# .option("uri", connectionString) \
# .option("collection", "stdev_rating_cuisine") \
# .save()

# stdevRatingByCuisine.show()

spark.stop()