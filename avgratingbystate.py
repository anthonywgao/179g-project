from statistics import fmean
from time import time
from pyspark.sql import SparkSession

# Lists for elapsed time 
oneCore = []
twoCore = []
threeCore = []
fourCore = []
fiveCore = []

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
    elif numExecutor == 5:
        fiveCore.append(elapseTime)

# Constants
database = "yelp_db"
collection = "business_collection"
host = "127.0.0.1"
port = "27017"

# Mongo connection URI
connectionString = "mongodb://{0}:{1}/{2}.{3}".format(host, port, database, collection)\

#spark session to check number of lines in data frame
sparkReader = SparkSession.builder \
            .appName("yelp_db") \
            .config("spark.mongodb.input.uri", connectionString) \
            .config("spark.mongodb.output.uri", connectionString) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
            .getOrCreate()
rowsBusinessDF = sparkReader.read.format("com.mongodb.spark.sql.DefaultSource").load().count()
print(f'Total number of rows: {rowsBusinessDF}')
sparkReader.stop()

# Run task 10 times for avg data collection
for i in range(10):
    # Partition data into fourths and check execution time
    for j in range(1,5):
        # Calculate row limit 
        dataLimit = int(rowsBusinessDF*j*0.25)
        print(f'\nRunning spark job with {j}/4 of data: {dataLimit} rows')

        # Run job with 1-5 executors
        for k in range(1,6):
            # Create spark session 
            spark = SparkSession.builder \
                .appName("yelp_db") \
                .config("spark.mongodb.input.uri", connectionString) \
                .config("spark.mongodb.output.uri", connectionString) \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.3.1') \
                .master(f'local[{k}]') \
                .getOrCreate()

            t0 = time()

            #Read from spark session input uri 
            reviewDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").load().limit(dataLimit)
            #Find average rating by state
            avgRatingByState = reviewDF.groupBy("state").agg({'stars':'avg'})

            #Write result to mongo in new collection of same database
            avgRatingByState.write \
                .mode("overwrite") \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", connectionString) \
                .option("collection", "avg_rating_state") \
                .save()
            timeTaken = time() - t0
            print(f'{k} executors, time= {timeTaken}')
            appendData(k,timeTaken)

            # avgRatingByState.show()
            spark.stop()

# Splitting collected data by # workers and data size
oneCoreoneFourth = oneCore[0::4]
oneCoretwoFourth = oneCore[1::4]
oneCorethreeFourth = oneCore[2::4]
oneCorefourFourth = oneCore[3::4]

twoCoreoneFourth = twoCore[0::4]
twoCoretwoFourth = twoCore[1::4]
twoCorethreeFourth = twoCore[2::4]
twoCorefourFourth = twoCore[3::4]

threeCoreoneFourth = threeCore[0::4]
threeCoretwoFourth = threeCore[1::4]
threeCorethreeFourth = threeCore[2::4]
threeCorefourFourth = threeCore[3::4]

fourCoreoneFourth = fourCore[0::4]
fourCoretwoFourth = fourCore[1::4]
fourCorethreeFourth = fourCore[2::4]
fourCorefourFourth = fourCore[3::4]

fiveCoreoneFourth = fiveCore[0::4]
fiveCoretwoFourth = fiveCore[1::4]
fiveCorethreeFourth = fiveCore[2::4]
fiveCorefourFourth = fiveCore[3::4]

print(f'1 worker 1/4 data: {oneCoreoneFourth} with mean: {fmean(oneCoreoneFourth)}')
print(f'2 worker 1/4 data: {twoCoreoneFourth} with mean: {fmean(twoCoreoneFourth)}')
print(f'3 worker 1/4 data: {threeCoreoneFourth} with mean: {fmean(threeCoreoneFourth)}')
print(f'4 worker 1/4 data: {fourCoreoneFourth} with mean: {fmean(fourCoreoneFourth)}')
print(f'5 worker 1/4 data: {fiveCoreoneFourth} with mean: {fmean(fiveCoreoneFourth)}')

print(f'1 worker 2/4 data: {oneCoretwoFourth} with mean: {fmean(oneCoretwoFourth)}')
print(f'2 worker 2/4 data: {twoCoretwoFourth} with mean: {fmean(twoCoretwoFourth)}')
print(f'3 worker 2/4 data: {threeCoretwoFourth} with mean: {fmean(threeCoretwoFourth)}')
print(f'4 worker 2/4 data: {fourCoretwoFourth} with mean: {fmean(fourCoretwoFourth)}')
print(f'5 worker 2/4 data: {fiveCoretwoFourth} with mean: {fmean(fiveCoretwoFourth)}')

print(f'1 worker 3/4 data: {oneCorethreeFourth} with mean: {fmean(oneCorethreeFourth)}')
print(f'2 worker 3/4 data: {twoCorethreeFourth} with mean: {fmean(twoCorethreeFourth)}')
print(f'3 worker 3/4 data: {threeCorethreeFourth} with mean: {fmean(threeCorethreeFourth)}')
print(f'4 worker 3/4 data: {fourCorethreeFourth} with mean: {fmean(fourCorethreeFourth)}')
print(f'5 worker 3/4 data: {fiveCorethreeFourth} with mean: {fmean(fiveCorethreeFourth)}')

print(f'1 worker 4/4 data: {oneCorefourFourth} with mean: {fmean(oneCorefourFourth)}')
print(f'2 worker 4/4 data: {twoCorefourFourth} with mean: {fmean(twoCorefourFourth)}')
print(f'3 worker 4/4 data: {threeCorefourFourth} with mean: {fmean(threeCorefourFourth)}')
print(f'4 worker 4/4 data: {fourCorefourFourth} with mean: {fmean(fourCorefourFourth)}')
print(f'5 worker 4/4 data: {fiveCorefourFourth} with mean: {fmean(fiveCorefourFourth)}')
