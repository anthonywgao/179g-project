# This file will apply the sentiment model to the yelp reviews
import re
import string
from pathlib import Path
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVCModel
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StopWordsRemover
from time import time
from statistics import fmean

# Lists for elapsed time 
oneCore = []
twoCore = []
threeCore = []
fourCore = []

#helper to collect data
def appendData(numExecutor, elapseTime):
    if numExecutor == 1:
        oneCore.append(elapseTime)
    elif numExecutor == 2:
        twoCore.append(elapseTime)
    elif numExecutor == 3:
        threeCore.append(elapseTime)
    elif numExecutor == 4:
        fourCore.append(elapseTime)

# Constants
database = "yelp_db"
collection = "review_collection"
host = "127.0.0.1"
port = "27017"
model_path = Path("test/models")

# Mongo connection URI
connectionString = "mongodb://{0}:{1}/{2}.{3}".format(host, port, database, collection)

#spark session to check number of lines in data frame
sparkReader = SparkSession.builder \
            .appName("yelp_db") \
            .config("spark.mongodb.input.uri", connectionString) \
            .config("spark.mongodb.output.uri", connectionString) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
            .getOrCreate()
rowsReviewDF = sparkReader.read.format("com.mongodb.spark.sql.DefaultSource").load().count()
print(f'Total number of rows: {rowsReviewDF}')
sparkReader.stop()

# Run task 3 times for avg data collection
for i in range(3):
    for j in range(1,4):
        # Calculate row limit 
        dataLimit = int((rowsReviewDF*j)/3)
        print(f'\nRunning spark job with {j}/3 of data: {dataLimit} rows')
        for k in range(1,5):
            # Build Spark session instance
            spark = SparkSession.builder \
                .appName("yelp_db") \
                .config("spark.mongodb.input.uri", connectionString) \
                .config("spark.mongodb.output.uri", connectionString) \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
                .master(f'local[{k}]') \
                .getOrCreate()

            t0 = time()

            # Load ml model
            lsvc_model = LinearSVCModel.load(str(model_path))

            # User defined functions 
            # Remove punctuation, whitespace, digits
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

            # Load data from db
            review_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load().limit(dataLimit)

            #remove unnecessary columns [6990280 reviews]
            cleaning_df = review_df.select('review_id', 'business_id', remove_punctuation_udf('text'), convert_rating_udf('stars'))

            cleaned_df = cleaning_df.withColumnRenamed('<lambda>(text)', 'text') \
                                    .withColumnRenamed('<lambda>(stars)', 'label', )

            print("Cleaned reviews")

            # tokenizer, stopword remover, hashtingTF, idf feature extraction
            tokenizer = Tokenizer(inputCol="text", outputCol="words")
            stop_words_remover = StopWordsRemover(inputCol="words", outputCol="words_no_stop")
            hashingTF = HashingTF(inputCol="words_no_stop", outputCol="rawfeatures", numFeatures=20)
            idf = IDF(inputCol="rawfeatures", outputCol="features")
            pipeline = Pipeline(stages=[tokenizer, stop_words_remover, hashingTF, idf])

            #Featurize text with tf-idf
            idf_df = pipeline.fit(cleaned_df).transform(cleaned_df).select('review_id', 'business_id', 'label', 'features')
            print("Featurized review text")

            # Predict sentiment with ml model
            pred = lsvc_model.transform(idf_df)
            print("Predict review sentiment")

            reviews_sentiment_df = pred.select('review_id', 'business_id', 'label', 'prediction') \
                                    .withColumnRenamed('label', "rating_sentiment") \
                                    .withColumnRenamed('prediction', "predicted_sentiment")

            print("Clean review sentiment")                       

            # Write results back to db
            reviews_sentiment_df.write \
                .mode("overwrite") \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", connectionString) \
                .option("collection", "review_sentiment") \
                .save()

            timeTaken = time() - t0
            print(f'{k} executors, time= {timeTaken}\n')
            appendData(k,timeTaken)
            spark.stop()

# Splitting collected data by # workers and data size
oneCoreoneThird = oneCore[0::3]
oneCoretwoThird = oneCore[1::3]
oneCorethreeThird = oneCore[2::3]

twoCoreoneThird = twoCore[0::3]
twoCoretwoThird = twoCore[1::3]
twoCorethreeThird = twoCore[2::3]

threeCoreoneThird = threeCore[0::3]
threeCoretwoThird = threeCore[1::3]
threeCorethreeThird = threeCore[2::3]

fourCoreoneThird = fourCore[0::3]
fourCoretwoThird = fourCore[1::3]
fourCorethreeThird = fourCore[2::3]

print(f'1 worker 1/3 data: {oneCoreoneThird} with mean: {fmean(oneCoreoneThird)}')
print(f'2 worker 1/3 data: {twoCoreoneThird} with mean: {fmean(twoCoreoneThird)}')
print(f'3 worker 1/3 data: {threeCoreoneThird} with mean: {fmean(threeCoreoneThird)}')
print(f'4 worker 1/3 data: {fourCoreoneThird} with mean: {fmean(fourCoreoneThird)}')

print(f'1 worker 2/3 data: {oneCoretwoThird} with mean: {fmean(oneCoretwoThird)}')
print(f'2 worker 2/3 data: {twoCoretwoThird} with mean: {fmean(twoCoretwoThird)}')
print(f'3 worker 2/3 data: {threeCoretwoThird} with mean: {fmean(threeCoretwoThird)}')
print(f'4 worker 2/3 data: {fourCoretwoThird} with mean: {fmean(fourCoretwoThird)}')

print(f'1 worker 3/3 data: {oneCorethreeThird} with mean: {fmean(oneCorethreeThird)}')
print(f'2 worker 3/3 data: {twoCorethreeThird} with mean: {fmean(twoCorethreeThird)}')
print(f'3 worker 3/3 data: {threeCorethreeThird} with mean: {fmean(threeCorethreeThird)}')
print(f'4 worker 3/3 data: {fourCorethreeThird} with mean: {fmean(fourCorethreeThird)}')
