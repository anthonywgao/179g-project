# This file will run sentiment analysis on Yelp review data

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml import Pipeline
from pyspark.mllib.classification import SVMWithSGD

# Constants
database = "yelp_db"
collection = "review_collection"
host = "127.0.0.1"
port = "27017"

# Mongo connection URI
connectionString = "mongodb://{0}:{1}/{2}.{3}".format(host, port, database, collection)

# Build Spark session instance
spark = SparkSession.builder \
    .appName("yelp_db") \
    .config("spark.driver.memory", "5g") \
    .config("spark.mongodb.input.uri", connectionString) \
    .config("spark.mongodb.output.uri", connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

# Load review data
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Print schema
df.printSchema()

import string
import re

# Use a regular expression to remove punctuation from review strings
def remove_punctuation(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\r\\t\\n]')
    return regex.sub(" ", text)

# Convert ratings to binary classification
def convert_rating(rating):
    if rating >= 4:
        return 1
    else:
        return 0

from pyspark.sql.functions import udf

remove_punctuation_udf = udf(lambda x: remove_punctuation(x))
convert_rating_udf = udf(lambda x: convert_rating(x))

df = df.select('review_id', remove_punctuation_udf('text'), convert_rating_udf('stars')).limit(1500000)

df = df.withColumnRenamed('<lambda>(text)', 'text')
df = df.withColumnRenamed('<lambda>(stars)', 'stars')

from pyspark.ml.feature import *

tokenizer = Tokenizer(inputCol="text", outputCol="words")
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="words_no_stop")

pipeline = Pipeline(stages=[tokenizer, stop_words_remover])
df = pipeline.fit(df).transform(df).cache()

# Build final TF-IDF feature matrix
cv = CountVectorizer(inputCol="words_no_stop", outputCol="tf_idf")
cv_model = cv.fit(df)
df = cv_model.transform(df)

#tf_idf_model = idf.fit(df)
#df = tf_idf_model.transform(df)

# Split data into train and test sets
#splits = df.select(['tf_idf', 'label']).randomSplit([0.8, 0.2], seed=100)
splits = df.select(['tf_idf', 'stars']).randomSplit([0.8, 0.2], seed=100)
train = splits[0].cache()
test = splits[1].cache()

# SVM model
iters = 50
regParam = 0.3
svm = SVMWithSGD.train(train, iters, regParam=regParam)

test_lb = test.rdd.map(lambda row: LabeledPoint(row[1], MLLibVectors.fromML(row[0])))
score_and_labels_test = test_lb.map(lambda x: (float(svm.predict(x.features)), x.label))
score_label_test = spark.createDataFrame(score_and_labels_test, ["prediction", "label"])

# F1 score
f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
svm_f1 = f1_eval.evaluate(score_label_test)
print("F1 score: %.4f" % svm_f1)