from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, weekofyear, year, month, window, count, lag, first, last, desc, row_number 
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd
import numpy as np

sc = SparkContext()
spark = SparkSession.builder.appName('Problem 3').getOrCreate()
# ----------- DATA PREP ----------
# Loading in data
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/sample_crime_data.csv'

# Using inferSchema with the appropriate timestampFormat to make sure the Date format is parsed as a timestamp
df = spark.read.csv(csv_path, header = True, inferSchema = True, timestampFormat='MM/DD/YYYY HH:MM:SS a')
# -----------
# Removing some unneeded columns (and columns whose data is at a lower granularity than a beat or would otherwise be unavailable for a future week count in a specific beat)
df = df.drop('X Coordinate','Y Coordinate','Updated On', 'Latitude', 'Longitude', 'Location', '_c0', 'Case Number', 'Community Area', 'Arrest','Domestic', 'Block', 'Description', 'IUCR', 'Primary Type', 'Location Description', 'FBI Code', 'Year', 'Ward')
# Dropping all rows with null values in any column
df = df.na.drop()
# -----------
# Truncating Date column for more robust extraction of week
df = df.withColumn('Date', df['Date'].cast('date'))

# +--------+----------+----+--------+
# |      ID|      Date|Beat|District|
# +--------+----------+----+--------+
# |11231348|2017-01-05|2233|      22|
# |11051313|2015-01-01| 726|       7|
# |11236344|2017-01-17| 812|       8|
# |10342577|2015-01-12| 713|       7|

# ----------- FEATURE GENERATION ----------
# Aggregating over a timestamp column with 'window' function
df_window = df.groupBy(window('Date', '1 week'), 'Beat', 'District').agg(count('ID').alias('crime_count')).orderBy('window')

# +------------------------------------------+----+--------+-----------+
# |window                                    |Beat|District|crime_count|
# +------------------------------------------+----+--------+-----------+
# |[2000-12-27 18:00:00, 2001-01-03 18:00:00]|1422|14      |2          |
# |[2000-12-27 18:00:00, 2001-01-03 18:00:00]|1212|12      |1          |
# |[2000-12-27 18:00:00, 2001-01-03 18:00:00]|733 |7       |2          |
# |[2000-12-27 18:00:00, 2001-01-03 18:00:00]|1222|12      |1          |
# |[2000-12-27 18:00:00, 2001-01-03 18:00:00]|2532|25      |2          |

# Creating lagged version of crime_count i.e. crime_Count 1 week ago
w = Window.partitionBy("Beat").orderBy("window.start")
df_window = df_window.withColumn('lagged_crime_count', lag(col('crime_count'), 1).over(w))

# +------------------------------------------+----+--------+-----------+------------------+
# |window                                    |Beat|District|crime_count|lagged_crime_count|
# +------------------------------------------+----+--------+-----------+------------------+
# |[2001-01-03 18:00:00, 2001-01-10 18:00:00]|833 |8       |7          |null              |
# |[2001-01-10 18:00:00, 2001-01-17 18:00:00]|833 |8       |3          |7                 |
# |[2001-01-17 18:00:00, 2001-01-24 18:00:00]|833 |8       |2          |3                 |
# |[2001-01-24 18:00:00, 2001-01-31 18:00:00]|833 |8       |3          |2                 |

# Lagging by 2
df_window = df_window.withColumn('lagged_2_crime_count', lag(col('crime_count'), 2).over(w))

# Lagging by 1 year (52 weeks)
df_window = df_window.withColumn('last_year_crime_count', lag(col('crime_count'), 52).over(w))

# +------------------------------------------+----+--------+-----------+------------------+--------------------+---------------------+
# |window                                    |Beat|District|crime_count|lagged_crime_count|lagged_2_crime_count|last_year_crime_count|
# +------------------------------------------+----+--------+-----------+------------------+--------------------+---------------------+
# |[2001-01-03 18:00:00, 2001-01-10 18:00:00]|833 |8       |7          |null              |null                |null                 |
# |[2001-01-10 18:00:00, 2001-01-17 18:00:00]|833 |8       |3          |7                 |null                |null                 |
# |[2001-01-17 18:00:00, 2001-01-24 18:00:00]|833 |8       |2          |3                 |7                   |null                 |
# |[2001-01-24 18:00:00, 2001-01-31 18:00:00]|833 |8       |3          |2                 |3                   |null                 |

# Dropping all rows with null values in any column (~5% of data, because we have 20 years and are losing 1)
df_window = df_window.na.drop()

# ----------- TRAIN & TEST SPLIT ----------

# Test data is the data for the last week for each Beat
w_desc = Window.partitionBy("Beat").orderBy(desc("window.start")) # Order in descending order of start of week (latest on top)
test = df_window.withColumn("rn", row_number().over(w_desc)).filter("rn == 1").drop('rn', 'window') # Add row numbers and take only row 1 i.e. the latest row for a specific beat

# +----+--------+-----------+------------------+--------------------+---------------------+
# |Beat|District|crime_count|lagged_crime_count|lagged_2_crime_count|last_year_crime_count|
# +----+--------+-----------+------------------+--------------------+---------------------+
# |833 |8       |1          |1                 |3                   |2                    |
# |623 |6       |2          |1                 |4                   |5                    |
# |1522|15      |1          |3                 |2                   |4                    |
# |2525|25      |1          |4                 |3                   |1                    |

# Train data is everything except the top row in each group (beat)
w_desc = Window.partitionBy("Beat").orderBy(desc("window.start"))
train = df_window.withColumn("rn", row_number().over(w_desc)).filter("rn != 1").drop('rn', 'window')

# +----+--------+-----------+------------------+--------------------+---------------------+
# |Beat|District|crime_count|lagged_crime_count|lagged_2_crime_count|last_year_crime_count|
# +----+--------+-----------+------------------+--------------------+---------------------+
# |833 |8       |1          |3                 |3                   |5                    |
# |833 |8       |3          |3                 |2                   |2                    |
# |833 |8       |3          |2                 |2                   |6                    |
# |833 |8       |2          |2                 |1                   |2                    |

# ----------- PIPELINE & EVALUATION ----------
# Reordering columns in format X, y
train = train.select('Beat', 'District', 'lagged_crime_count', 'lagged_2_crime_count', 'last_year_crime_count', 'crime_count')
test = test.select('Beat', 'District', 'lagged_crime_count', 'lagged_2_crime_count', 'last_year_crime_count', 'crime_count')

# Casting Beat & District columns to string
test = test.withColumn("Beat", test["Beat"].cast('string'))
test = test.withColumn("District", test["District"].cast('string'))
train = train.withColumn("Beat", train["Beat"].cast('string'))
train = train.withColumn("District", train["District"].cast('string'))

# StringIndexing the Beat & District categorical variables
beatIndexer = StringIndexer(inputCol='Beat', outputCol='BeatIdx')
districtIndexer = StringIndexer(inputCol='District', outputCol='DistrictIdx')

# One-Hot Encoding the StringIdx variables
encoder = OneHotEncoderEstimator(inputCols=["BeatIdx","DistrictIdx"],
                                 outputCols=["BeatVec","DistrictVec"], handleInvalid = 'keep')

# Assembling all the features
vectorAssembler = VectorAssembler(inputCols = ["BeatVec", "DistrictVec", "lagged_crime_count", "lagged_2_crime_count", "last_year_crime_count"], outputCol = 'features')

# Linear Regression
lr = LinearRegression(featuresCol = "features", labelCol = "crime_count")
# Random Forest Regressor
rf = RandomForestRegressor(featuresCol = "features", labelCol = "crime_count")

# Pipeline
# Linear Regression
pipeline_lr = Pipeline(stages = [beatIndexer, districtIndexer, encoder, vectorAssembler, lr])
model_lr = pipeline_lr.fit(train)
predictions_lr = model_lr.transform(test)

# Random Forest Regressor
pipeline_rf = Pipeline(stages = [beatIndexer, districtIndexer, encoder, vectorAssembler, rf])
model_rf = pipeline_rf.fit(train)
predictions_rf = model_rf.transform(test)

# I think it would be better if I have just one pipeline for data prep, 
# as opposed to replicating the same operations twice as above.
# Then do the model fitting (lr & rf) separately. 
# However, I wanted to avoid experimenting with this to avoid adding load to the server.
# (And I wasn't certain if the assignment requires us to include all the steps in the pipeline)

# ----------- EVALUATION ----------
evaluator = RegressionEvaluator(predictionCol = 'prediction', labelCol = 'crime_count')
result_lr = evaluator.evaluate(predictions_lr)
result_rf = evaluator.evaluate(predictions_rf)

results = [result_lr, result_rf]

with open("dimitrov_exercise3_output.txt", 'w') as output:
    for result in results:
    	output.write(str(result) + '\n')

predictions_lr = predictions_lr.select('Beat', 'prediction', 'crime_count')
predictions_rf = predictions_rf.select('Beat', 'prediction', 'crime_count')

predictions_lr.write.csv('dimitrov_exercise3_lr_predictions.csv')
predictions_rf.write.csv('dimitrov_exercise3_rf_predictions.csv')
# The above produced folders, which contained the results from all the partitions in a separate file
# I used getmerge, to bring the files to Wolf and merge them.

# I could have also used:
# predictions_lr.coalesce(1).write.csv('dimitrov_exercise3_lr_predictions.csv')
# predictions_rf.coalesce(1).write.csv('dimitrov_exercise3_rf_predictions.csv')

# RESULTS:
# The Evaluation (SSE) of the Linear Regression & the Random Forest Regressor are in dimitrov_exercise3_output:
# LR: 129.35431589555915
# RF: 119.03012837819135

# Therefore, the Random Forest fits the data better.
# The predictions for each are in two .txt files in csv format for 'Beat', 'prediction', 'crime_count'


