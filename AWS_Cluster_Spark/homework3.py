
# #### WHAT HAPPENED WHEN I RAN MY JOB ON THE EMR CLUSTER #####

# - I ran my job on an EMR cluster and it failed with this error:
# Total size of serialized results of 18892 tasks (1024.0 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)

# - The line specified as causing the error is : model = pipeline.fit(df_features)
# - However, from experimenting, I have found that the error is actually from:
#     df_with_new_column = df_1.join(results, on = ['trade_id', 'bar_num']) 

# - This is where I join the results from my for loop with my original dataframe i.e. when I add the newly calculated feature


##### WHAT I WOULD DO IF I HAD MORE TIME (AND SLEEP) #####

# - I would extract year & month from the time_stamp column and make an outer for loop which repeats the below process of training a model and then testing it
# - I am aware that below I am running the predictions on the training data (the 6 months instead of the 1 month)
#     - I did this to make sure my script runs completely and the MAPE calculation is correct.
# - Experiment with the recursive approach for defining features, both based on profit and on the remaining features (which are probably easier, because of lighter restrictions)


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.functions import col, weekofyear, year, month, window, count, lag, first, last, desc 
from operator import add
import pandas as pd

print("----- IMPORTS COMPLETE!")

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Problem 3').getOrCreate()

    # sc = SparkContext(appName="myApp")
    # sc.setLogLevel('ERROR')
    # sqlcontext = SQLContext(sc)

    print("Reading data")
    csv_path = "s3://msia423-homework3-ktd5131/data.csv"
    df = spark.read.csv(csv_path, header = True , inferSchema = True, timestampFormat='YYYY-MM-DD HH:MM:SS a')
    print("read data!")


    # CONFIGURATION
    print("Setting alpha = .2")
    alpha = .2


    ## This is the EWMA function, which takes a list of values and an alpha parameters and calculates the exponentially weighted average
    ## Testing EWMA with a range of values
    def ewma_lst(alpha, lst):
        
        res = 0
        
        for ii in range(len(lst)):
            res += alpha * ( (1-alpha)**ii ) * lst[ii]
        
        return res

    # Register UDF
    ewma = spark.udf.register("ewma_lst", ewma_lst)

    print('DEFINED UDF')
    
    print("Taking only data after 2008, before that is not relevant")
    # Taking only data after 2008, before that is not relevant
    df_2008 = df.filter(df.time_stamp >= '2008-01-01 00:00:00')

    print("Dropping 'direction' column")
    # Dropping 'direction' column
    df_2008 = df_2008.drop('direction')

    print("Taking first chunk of data; NEED TO PARAMETRIZE WITH YEAR & MONTH LATER ")
    # Taking first chunk of data; NEED TO PARAMETRIZE WITH YEAR & MONTH LATER 
    df_1 = df_2008.filter(df_2008.time_stamp <= '2008-06-01 00:00:00')

    print("Let's take a subset of columns for ease")
    # Let's take a subset of columns for ease
    df_subset = df_1.select(['bar_num', 'profit', 'trade_id'])


    print("FINDING max_bar")
    # Verifying the max bar value across all trades
    max_bar_per_trade = df_1.groupBy(col("trade_id")).agg({"bar_num": "max"}).alias('max_bar_num')
    max_bar = max_bar_per_trade.agg({"max(bar_num)": "max"}).collect()[0][0]
    # Turns out to be 120 i.e. there are a maximum of 120 bars for all trades
    # Further investigations show that NOT ALL bars have all 120 bars


    print("Create schema for empty dataframe, which will hold all the calculated ewma profits")
    # Create schema for empty dataframe, which will hold all the calculated ewma profits
    schema = StructType([StructField('trade_id', LongType(), False),
                     StructField('profit_ewma', DoubleType(), False), 
                     StructField('bar_num', LongType(), True)])

    results = spark.createDataFrame([], schema)


    df_subset.cache()
    print("DATAFRAME CACHED")


    for ii in range(11, max_bar): # For bars 1, 2, 3, ... 10, we don't need to do anything; So, when we do left join, those feature values for bars 1-10 should be null
    
        print(f'START {ii}')
        
        if ii % 10 == 0: # This means we are in the situation of taking bar 20, 30, 40, etc.
            bars_to_take = ii - 10 # For bar 20, we want bars 10 and below; for bar 30, we want bars 20 and below...
        else:
            bars_to_take = ii - ii%10 # E.g. if we are at bar 33, we want bars 33 - 3 = 30 and below
        
        print(f'BARS_TO_TAKE={bars_to_take}')


        print('Taking only the part of the dataset, which contains the subset of bars we are interested in')
        # Taking only the part of the dataset, which contains the subset of bars we are interested in
        df_filtered = df_subset.filter(f'bar_num <= {bars_to_take}')
           
        # EXAMPLE OUTPUT:
        # +-------+------+--------+
        # |bar_num|profit|trade_id|
        # +-------+------+--------+
        # |     10|   -16|    9853|
        # |      9|    16|    9853|
        # |      8|    -3|    9853|
        # +-------+------+--------+

        print('Collecting all the profits for a given trade_id in one place (list with its corresponding trade_id)') 
        # Collecting all the profits for a given trade_id in one place (list with its corresponding trade_id)
        df_intermediate = df_filtered.groupby('trade_id').agg(F.collect_list('profit'))
        
        # EXAMPLE OUTPUT:
        # +--------+--------------------------------------------------------------------------------------------------------------------------+
        # |trade_id|collect_list(profit)                                                                                                      |
        # +--------+--------------------------------------------------------------------------------------------------------------------------+
        # |9900    |[-24, -18, -18, -8, -3, 6, 7, 17, 17, 19, 28, 28, 17, 2, 8, 9, 30, 40, 47, 47, 5, -5, -20]                                |
        # |9852    |[-158, -135, -131, -100, -101, -73, -92, -50, -70, -80, -65, -69, -81, -51, -40, -41, -72, -94, -62, -32, -34, -9, -26]   |
        # |10081   |[60, 64, 60, 43, 21, 47, 32, 32, 74, 84, 82, 79, 79, 90, 50, 94, 40, 24, -6, 2, -1, -5, -25]                              |
        # |9879    |[-94, -99, -119, -124, -144, -149, -159, -93, -102, -104, -97, -73, -74, -84, -89, -74, -82, -55, -53, -51, -61, -33, -35]|
        # +--------+--------------------------------------------------------------------------------------------------------------------------+
            
        print('Apply the UDF EWMA function to the collected list of profits')
        # Apply the UDF EWMA function to the collected list of profits
        df_ewma = df_intermediate.select('trade_id', ewma(F.lit(alpha), col("collect_list(profit)")))

        # EXAMPLE OUTPUT
        # +--------+-----------------------------------+
        # |trade_id|ewma_lst(0.2, collect_list(profit))|
        # +--------+-----------------------------------+
        # |    9900|                 14.230217728000003|
        # |    9852|                -45.088219033600005|
        # |   10081|                  42.92491776000001|
        # +--------+-----------------------------------+

        print('Adding the bar_num we are currently at, so we can properly join later')
        # Adding the bar_num we are currently at, so we can properly join later
        final_df = df_ewma.withColumn('bar_num', lit(ii))

        # EXAMPLE OUTPUT:
        # +--------+-----------------------------------+-------+
        # |trade_id|ewma_lst(0.2, collect_list(profit))|bar_num|
        # +--------+-----------------------------------+-------+
        # |    9900|                 14.230217728000003|    119|
        # |    9852|                -45.088219033600005|    119|
        # |   10081|                  42.92491776000001|    119|
        # +--------+-----------------------------------+-------+

        print('RESULTS UNION FINAL_DF')
        results = results.union(final_df)   
        
        # EXAMPLE OUTPUT:
        # +--------+-------------------+-------+
        # |trade_id|        profit_ewma|bar_num|
        # +--------+-------------------+-------+
        # |    9900| 14.230217728000003|    119|
        # |    9852|-45.088219033600005|    119|
        # |   10081|  42.92491776000001|    119|
        # |    9879| -64.16702259200002|    119|
        # |   10121|-16.785674240000006|    119|
        # |    9946|  9.038351359999998|    119|
        # |   10032| 15.545000038400003|    119|
        # |    9775| 28.437628416000003|    119|
        # |    9914|  93.61254041600002|    119|
        # |   10090|      -37.236989952|    119|
        # |   10128| 22.083040563200004|    119|
        # |    9721|      -18.770504704|    119|
        # |   10013| 27.960904192000005|    119|
        # |    9925| 220.04457932800008|    119|
        # |    9731|-10.384396083199999|    119|
        # |   10143| -96.87878338560002|    119|
        # |   10183| -37.69082163200001|    119|
        # |    9867|-35.009529241600006|    119|
        # |   10195|-10.397627392000006|    119|
        # |    9898|  94.27531079680001|    119|
        # +--------+-------------------+-------+

        print(f'END {ii}')


    print("PERFORMING JOIN WITH ORIGINAL TABLE")

    df_with_new_column = df_1.join(results, on = ['trade_id', 'bar_num']) # I think this is the line where things break, based on experimentation

    print('DROPPING UNNECESSARY FEATURES')
    df_features = df_with_new_column.drop('time_stamp', 'bar_num', 'trade_id')


    print("Making sure profit_ewma is a float")
    df_features = df_features.withColumn("profit_ewma", df_features["profit_ewma"].cast(DoubleType()))


    print("DEFINING VECTOR INDEXER")
    vectorAssembler = VectorAssembler(inputCols = ['profit_ewma','var12','var13','var14','var15','var16','var17','var18','var23','var24','var25','var26','var27','var28','var34','var35','var36','var37','var38','var45','var46','var47','var48','var56','var57','var58','var67','var68','var78'], outputCol = 'features')


    print("DEFINING RANDOM FOREST REGRESSOR")
    rf = RandomForestRegressor(featuresCol = 'features', labelCol = 'profit', seed=42)

    print("DEFINING PIPELINE")
    pipeline = Pipeline(stages = [vectorAssembler, rf])

    print("Training model")
    model = pipeline.fit(df_features)

    print("Applying model to data (This is applying the model to the training data; for the homework I should apply to month 7 in 2008)")
    predictions = model.transform(df_features)

    print('TWO COLUMNS REQUIRED FOR MAPE')
    label_and_prediction = predictions.select('profit', 'prediction')

    print("ADDING SMALL NUMBER TO PROFIT")
    label_and_prediction = label_and_prediction.withColumn('profit', df.profit + 0.1) 

    print("CALCULATING percent_difference")
    MAPE = label_and_prediction.withColumn('percent_difference', expr("F.abs(profit - prediction)/profit"))

    print("CALCULATING AVERAGE")
    avg_MAPE = MAPE.select(F.mean(col('percent_difference')).alias('mean')).collect()

    mean = avg_MAPE[0]['mean']
    print(mean)

    sc.stop()
# samples.saveAsTextFile("hdfs://wolf.analytics.private/user/mtl8754/example/wc_spark/")


################ ERROR MESSAGE FROM STDOUT ####################

# Training model
# Traceback (most recent call last):
#   File "try_1.py", line 146, in <module>
#     model = pipeline.fit(df_features)
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/ml/base.py", line 132, in fit
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/ml/pipeline.py", line 109, in _fit
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/ml/base.py", line 132, in fit
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/ml/wrapper.py", line 295, in _fit
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/ml/wrapper.py", line 292, in _fit_java
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/py4j-0.10.7-src.zip/py4j/java_gateway.py", line 1257, in __call__
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
#   File "/mnt1/yarn/usercache/hadoop/appcache/application_1591936717110_0002/container_1591936717110_0002_02_000001/py4j-0.10.7-src.zip/py4j/protocol.py", line 328, in get_return_value
# py4j.protocol.Py4JJavaError: An error occurred while calling o2430.fit.

                ################ MAIN ERROR REASON ####################

# : org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 18892 tasks (1024.0 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)

                ################ MAIN ERROR REASON ####################

    # at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:2043)
    # at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:2031)
    # at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:2030)
    # at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
    # at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
    # at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2030)
    # at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:967)
    # at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:967)
    # at scala.Option.foreach(Option.scala:257)
    # at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:967)
    # at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2264)
    # at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2213)
    # at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2202)
    # at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
    # at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:778)
    # at org.apache.spark.SparkContext.runJob(SparkContext.scala:2061)
    # at org.apache.spark.SparkContext.runJob(SparkContext.scala:2082)
    # at org.apache.spark.SparkContext.runJob(SparkContext.scala:2101)
    # at org.apache.spark.rdd.RDD$$anonfun$take$1.apply(RDD.scala:1409)
    # at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    # at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    # at org.apache.spark.rdd.RDD.withScope(RDD.scala:385)
    # at org.apache.spark.rdd.RDD.take(RDD.scala:1382)
    # at org.apache.spark.ml.tree.impl.DecisionTreeMetadata$.buildMetadata(DecisionTreeMetadata.scala:112)
    # at org.apache.spark.ml.tree.impl.RandomForest$.run(RandomForest.scala:106)
    # at org.apache.spark.ml.regression.RandomForestRegressor$$anonfun$train$1.apply(RandomForestRegressor.scala:133)
    # at org.apache.spark.ml.regression.RandomForestRegressor$$anonfun$train$1.apply(RandomForestRegressor.scala:119)
    # at org.apache.spark.ml.util.Instrumentation$$anonfun$11.apply(Instrumentation.scala:185)
    # at scala.util.Try$.apply(Try.scala:192)
    # at org.apache.spark.ml.util.Instrumentation$.instrumented(Instrumentation.scala:185)
    # at org.apache.spark.ml.regression.RandomForestRegressor.train(RandomForestRegressor.scala:119)
    # at org.apache.spark.ml.regression.RandomForestRegressor.train(RandomForestRegressor.scala:46)
    # at org.apache.spark.ml.Predictor.fit(Predictor.scala:118)
    # at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    # at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    # at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    # at java.lang.reflect.Method.invoke(Method.java:498)
    # at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    # at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    # at py4j.Gateway.invoke(Gateway.java:282)
    # at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    # at py4j.commands.CallCommand.execute(CallCommand.java:79)
    # at py4j.GatewayConnection.run(GatewayConnection.java:238)
    # at java.lang.Thread.run(Thread.java:748)



























