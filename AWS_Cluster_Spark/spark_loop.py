#An example of the wrong way:

df = sqlcontext.red.csv('path')
df.cache()

max_value = df.select('column').groupBy().agg(max('column')).collect()[0][0] # This operation will cache the df

for i in range(1, max_value):
    df_subset = df.filter(f"column = {i}")
    df_subset = df_subset.withColumn(...)
    #operations and stuff
    #Below is just one bad example of a potential loop operation
    # df_subset2 = df_subset.groupBy('column').agg(mean('value').alias('value_mean'))
    # df_subset = df_subset.join(df_subset2, on = 'column')
    
    df = df.join(df_subset, on = 'idx', how = 'left')
    

# Why?
# Theoretically, this is fine as long as you cache df, right?
# nope.
# df itself might be cached, but every prior level of the loop will require execution to get the next value
# This means to get level 3, you need to execute levels 1 and 2. But to get level 4, you will need to execute levels 1, 2, and 3 again
# Can easily lead to GC Allocation failure: https://spark.apache.org/docs/latest/tuning.html#garbage-collection-tuning
# Cause: Referencing and overwriting df within a loop

#vIn python, we expect that once level 1 has been executed, we no longer need to worry about it. This is because python is in-memory
# In Spark, this is not the case. And level 4 as it has been written only exists because of levels 1, 2, and 3. 
# Level 5 also only exists because of levels 1, 2, 3, and 4. But here you are not telling spark to reference the dataframe which already
#     has levels 1, 2, 3, and 4. So it must remake it.
# For this reason, you should never reference or edit a dataframe within a loop. Only gather the information you need from it

#A much cleaner solution:
# I walk through an entire example here, but the key points are:
# 1: Create a schema for an empty dataframe
# 2: Restrict all operations of the cache dataframe to 'get' operations, like filters. 
#       The important thing is your source dataframe should never change.
# 3: Append the results of each level to your previously empty dataframe
# 4: Join your source dataframe to the results to collect features

#The example I walk through involves discount chaining
# Essentially, we have a case where someone could receive multiple coupons on a single item
# However, the value of the coupon is dependent on other active coupons

#two coupons with 9% discount
# first will be 100*.09 = $9
# second will be 91*.09 = $8.1
# third *could* be 91*.09 = $8.1 as well

df = (sqlcontext.read.csv('path')
        .withColumn('previous_deductions', lit(0))
        .withColumn('coupon_discount', lit(0))) #these two columns are placeholders that will be replaced by an eventual join
df.cache()

max_value = df.select('column').groupBy().agg(max('column')).collect()[0][0] # still executes the cache

#Here we are defining the schema of an empty dataframe that will grow as we traverse out loop
schema = StructType([StructField('idx', LongType(), False), #placeholder for transaction  (AKA trade_id)
                     StructField('coupon_id', StringType(), False), #placeholder for coupon id (AKA bar_bum)
                     StructField('previous_deductions', DoubleType(), True), #where we will hold the sum of rpevious deductions
                     StructField('coupon_discount', DoubleType(), True)]) #the actual discount for thsi program at this id

results = spark.createDataFrame([], schema)

for i in range(1,max_value):
    level = df.filter(f"value = {i}") #one filter on the cached dataframe

    if i>1:
        level = level.drop('previous_deductions')
        level = level.join(next_level, on = ['idx', 'level']) #you will see this later, it has values for previous_deductions
    
    apply_discounts = level.withColumn('coupon_discount', expr("discount_percent * (Cost - previous_deductions)"))

    #The next two pieces are only applicable to this exact example, but explain how to pass information between levels efficiently
    this_level = (apply_discounts.select('idx', 'coupon_discount', 'previous_deductions')
                                 .groupBy('idx', 'previous_deductions').agg(sum('coupon_discount').alias('coupon_discount')))
                                 #This might nto be applicable to you, but the idea here is that multiple coupons can apply at a given level
    
    next_level = (this_level.withColumn('sum_deductions', expr("previous_deductions + coupon_discount"))
                            .withColumn('level', lit(i+1)))
                            # This is a trick to pass values to the next level
                            # If there is no next level, this is still a valid method, because next_level will not join on null values

    #finally we select the desired values and union the schema
    explicit discounts = apply_discounts.select('idx', 'coupon_id', 'previous_deductions', 'coupon_discount')
    #be careful with plain union. It is more efficient than other union methods, but it is schema agnostic
    results = results.union(explicit_discounts)


df_with_new_column = df.join(results, on = ['idx', 'coupon_id'])
#tada!
df_woth_new_column.write(...) #actually executes

sc.stop()