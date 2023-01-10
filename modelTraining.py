# This file will train the sentiment analysis model on Yelp review data
import string
import re
import shutil
from time import time
from pathlib import Path
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import udf
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover


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
    .config("spark.driver.memory", "2g") \
    .config("spark.mongodb.input.uri", connectionString) \
    .config("spark.mongodb.output.uri", connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
    .getOrCreate()

# Load review data
t0 = time()
review_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Use a regular expression to remove punctuation from review strings
def remove_punctuation(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\s]')
    clean_text = re.sub('\\s{2,}', ' ', regex.sub(" ", text))
    return clean_text

# Convert ratings to binary classification
def convert_rating(rating):
    if rating >= 4:
        return 1
    else:
        return 0

# User defined functions 
remove_punctuation_udf = udf(lambda x: remove_punctuation(x))
convert_rating_udf = udf(lambda x: convert_rating(x), IntegerType())

#remove unnecessary columns [6990280 reviews]
cleaning_df = review_df.select('review_id', remove_punctuation_udf('text'), convert_rating_udf('stars'))

cleaned_df = cleaning_df.withColumnRenamed('<lambda>(text)', 'text') \
                        .withColumnRenamed('<lambda>(stars)', 'label', )

cleaned_df.show(5)

# tokenizer, stopword remover, hashtingTF, idf feature extraction
tokenizer = Tokenizer(inputCol="text", outputCol="words")
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="words_no_stop")
hashingTF = HashingTF(inputCol="words_no_stop", outputCol="rawfeatures", numFeatures=20)
idf = IDF(inputCol="rawfeatures", outputCol="features")

pipeline = Pipeline(stages=[tokenizer, stop_words_remover, hashingTF, idf]) 
idf_df = pipeline.fit(cleaned_df).transform(cleaned_df).select('review_id', 'label', 'features')
cleaned_df.unpersist()

idf_df.show(5)
# Split data into train and test sets
(train, test) = idf_df.select(['features', 'label']).randomSplit([0.8, 0.2], 50)
idf_df.unpersist()

# SVM model
iters = 50
regParam = 0.3
lsvc = LinearSVC(featuresCol="features", labelCol="label", maxIter=iters, regParam=regParam)
lsvc_model = lsvc.fit(train)

pred = lsvc_model.transform(test)

# save model
model_path = Path("test/models")
if model_path.exists() & model_path.is_dir():
    shutil.rmtree(model_path)
Path(model_path).mkdir(parents=True, exist_ok=True)
lsvc_model.write().overwrite().save(str(model_path))

#F1 score
f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
svm_f1 = f1_eval.evaluate(pred)
print("F1 score: %.4f" % svm_f1)

timeTaken = time() - t0
print(f'time= {timeTaken}')

spark.stop()