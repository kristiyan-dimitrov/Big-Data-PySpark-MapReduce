"""
I will train a Random Forest Classfier on the data
and inspect the feature importance as a way of investigating what
hour of day, day of week, and month have the most influence on whether
or not there will be an arrest.

I include multiple other variables to avoid 'lurking variables'

Note: There is an almost identical .py file where I do the same but with a Gradient Boosted Tree
The corresponding outputs are in two separate files as well.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, year, month, window, count, lag, first, last, desc, dayofweek, hour
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml import Pipeline
from itertools import chain
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('Problem 4').getOrCreate()

# Loading in data
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
# Using inferSchema with the appropriate timestampFormat to make sure the Date format is parsed as a timestamp
df = spark.read.csv(csv_path, header = True, inferSchema = True, timestampFormat='MM/DD/YYYY HH:MM:SS a')

# Removing some unneeded columns
df = df.drop('X Coordinate','Y Coordinate','Updated On', 'Latitude', 'Longitude', 'Location', '_c0', 'Case Number', 'ID', 'Block', 'Description', 'Location Description', 'FBI Code', 'Year')
# Dropping all rows with null values in any column
df = df.na.drop()

# ----- GENERATING FEATURES -------
# Extracting hour of day, day of week, and month from the Date
df = df.withColumn('Hour_of_day', hour('Date'))
df = df.withColumn('Day_of_week', dayofweek('Date'))
df = df.withColumn('Month', month('Date'))
df = df.drop('Date')

# Casting feature columns as String
df = df.withColumn("Domestic", df["Domestic"].cast('string'))
df = df.withColumn("Beat", df["Beat"].cast('string'))
df = df.withColumn("Day_of_week", df["Day_of_week"].cast('string'))
df = df.withColumn("Month", df["Month"].cast('string'))
df = df.withColumn("Hour_of_day", df["Hour_of_day"].cast('string'))
df = df.withColumn("District", df["District"].cast('string'))
df = df.withColumn("Ward", df["Ward"].cast('string'))
df = df.withColumn("Community Area", df["Community Area"].cast('string'))

# Apparently the label needs to be numeric
df = df.withColumn("Arrest", df["Arrest"].cast('integer'))

# df.show()
# +----+--------------------+------+--------+----+--------+----+--------------+-----------+-----------+-----+
# |IUCR|        Primary Type|Arrest|Domestic|Beat|District|Ward|Community Area|Hour_of_day|Day_of_week|Month|
# +----+--------------------+------+--------+----+--------+----+--------------+-----------+-----------+-----+
# |1153|  DECEPTIVE PRACTICE|     0|   false|2233|      22|  34|            75|         12|          5|    1|
# |1753|OFFENSE INVOLVING...|     0|   false| 726|       7|  15|            67|         12|          5|    1|
# |1153|  DECEPTIVE PRACTICE|     0|   false| 812|       8|  23|            64|          8|          3|    1|
# |2027|           NARCOTICS|     1|   false| 713|       7|  16|            67|          1|          2|    1|

# ----- PIPELINE -------
# Creating Indexers for all 10 categorical features
BeatIdx = StringIndexer(inputCol='Beat', outputCol='BeatIdx')
DistrictIdx = StringIndexer(inputCol='District', outputCol='DistrictIdx')
DomesticIdx = StringIndexer(inputCol='Domestic', outputCol='DomesticIdx')
Primary_TypeIdx = StringIndexer(inputCol='Primary Type', outputCol='Primary_TypeIdx')
Community_AreaIdx = StringIndexer(inputCol='Community Area', outputCol='Community_AreaIdx')
WardIdx = StringIndexer(inputCol='Ward', outputCol='WardIdx')
IUCRIdx = StringIndexer(inputCol='IUCR', outputCol='IUCRIdx')
Day_of_weekIdx = StringIndexer(inputCol='Day_of_week', outputCol='Day_of_weekIdx')
Hour_of_dayIdx = StringIndexer(inputCol='Hour_of_day', outputCol='Hour_of_dayIdx')
MonthIdx = StringIndexer(inputCol='Month', outputCol='MonthIdx')

# Creating Indexer for response variable (label)
ArrestIdx = StringIndexer(inputCol='Arrest', outputCol='ArrestIdx')

# One Hot Encoding
encoder = OneHotEncoderEstimator(inputCols=["BeatIdx","DistrictIdx", "DomesticIdx","Primary_TypeIdx", "Community_AreaIdx", "WardIdx", "Hour_of_dayIdx","IUCRIdx", "Day_of_weekIdx", "MonthIdx"],
                                 outputCols=["BeatVec","DistrictVec", "DomesticVec","Primary_TypeVec", "Community_AreaVec", "WardVec", "Hour_of_dayVec","IUCRVec", "Day_of_weekVec", "MonthVec"], handleInvalid = 'keep')

# Assembiling features
vectorAssembler = VectorAssembler(inputCols = ["BeatVec","DistrictVec", "DomesticVec","Primary_TypeVec", "Community_AreaVec", "WardVec", "Hour_of_dayVec","IUCRVec", "Day_of_weekVec", "MonthVec"], outputCol = 'features')

# Gradient Boosted Tree Classifier
rf = RandomForestClassifier(featuresCol = "features", labelCol = "ArrestIdx")

# Pipeline
pipeline = Pipeline(stages = [BeatIdx, DistrictIdx, DomesticIdx, Primary_TypeIdx, Community_AreaIdx, WardIdx, Hour_of_dayIdx, IUCRIdx, Day_of_weekIdx, MonthIdx, ArrestIdx, encoder, vectorAssembler, rf])

# Training model
model = pipeline.fit(df)

# Applying model to data
predictions = model.transform(df)

# predictions.select('rawPrediction', 'probability','prediction', 'ArrestIdx').show()
# +--------------------+--------------------+----------+---------+
# |       rawPrediction|         probability|prediction|ArrestIdx|
# +--------------------+--------------------+----------+---------+
# |[0.77552666957185...|[0.82506581466870...|       0.0|      0.0|
# |[0.77552666957185...|[0.82506581466870...|       0.0|      0.0|
# |[0.77552666957185...|[0.82506581466870...|       0.0|      0.0|
# |[-1.4662519710004...|[0.05056996866837...|       1.0|      1.0|
# |[0.77552666957185...|[0.82506581466870...|       0.0|      0.0|
# |[-0.1219806630523...|[0.43931037632434...|       1.0|      0.0|

# Extracting the names of the features
attrs = sorted(
    (attr["idx"], attr["name"]) for attr in (chain(*predictions
        .schema["features"]
        .metadata["ml_attr"]["attrs"].values())))

# Combining the names of the features with their corresponding values in the featureImportances property of the model object
feature_importances = [(name, model.stages[-1].featureImportances[idx]) for idx, name in attrs if model.stages[-1].featureImportances[idx]]

# Evaluating model with AUC
evaluator_AUC = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol = 'ArrestIdx')
AUC = evaluator_AUC.evaluate(predictions)

# Evaluating the model with Accuracy
evaluator_Accuracy = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol = 'ArrestIdx', metricName = 'accuracy')
accuracy = format(evaluator_Accuracy.evaluate(predictions), "%")

# Writing output
with open("dimitrov_exercise4_rf_output.txt", 'w') as output:
    output.write("The Area under the ROC of the RandomForestClassifier is: " + str(AUC) + '\n')
    output.write("The Accuracy of the RandomForestClassifier is: " + accuracy + '\n')
    output.write("List of Feature Importances: " + '\n' + '\n')
    
    for feature_importance in feature_importances:
    	output.write(str(feature_importance) + '\n')

# COMMENTS
# I will look at the results from GBT, because it proved to be the more accurate model:

# HOUR OF DAY:
# ('Hour_of_dayVec_12', 2.473033531972289e-06)
# ('Hour_of_dayVec_9', 2.1123366489401568e-05)
# ('Hour_of_dayVec_10', 8.074152033930924e-06)
# ('Hour_of_dayVec_8', 3.4069067576306586e-05)
# ('Hour_of_dayVec_11', 1.1152633553656263e-05)
# ('Hour_of_dayVec_7', 8.935140287087282e-06)
# ('Hour_of_dayVec_1', 0.00014199095573777176)
# ('Hour_of_dayVec_2', 3.689939661407015e-05)
# ('Hour_of_dayVec_3', 2.4064361105592106e-05)
# ('Hour_of_dayVec_6', 8.289663684398513e-06)
# ('Hour_of_dayVec_5', 5.423833165420395e-07)

# Highest importance to 2 & 3 as well as 9 & 10.
# The 2 & 3 are arrests during the night
# While 9 & 10 are arrests when "people are having fun"

# DAY OF WEEK:
# ('Day_of_weekVec_3', 3.911275203522637e-06)
# ('Day_of_weekVec_4', 2.680064925619146e-06)
# ('Day_of_weekVec_1', 4.278605627356926e-06)
# ('Day_of_weekVec_6', 1.853886699495846e-06)
# ('Day_of_weekVec_7', 8.270546643471742e-06)
# ('Day_of_weekVec_2', 2.4247778720013115e-06)

# Highest importance on Sunday 

# I am not certain why both models ignored the Month variable.
# Maybe the variable is not predictive of an arrest
# Which would mean that the month of year doesn't influence the probability of there being an arrest
# That doesn't seem too unlikely





