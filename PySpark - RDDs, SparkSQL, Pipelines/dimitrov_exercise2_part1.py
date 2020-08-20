from pyspark import SparkContext
from pyspark.sql import SQLContext
from operator import add
import pandas as pd

sc = SparkContext()
sqlcontext = SQLContext(sc)

# PART 1
# Loading Data
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/sample_crime_data.csv'
rddd = sc.textFile(csv_path)

# Getting only the blocks data and placing it in the right format. Giving 0 to years different from last 3
blocksRDD = rddd.map(lambda x: (x.split(",")[3], 1) if x.split(",")[17] in {'2018', '2019', '2020'} else (x.split(",")[3], 0))
# Sum all the crimes per block
casesPerBlockRDD = blocksRDD.reduceByKey(add)
# Sort by number
sortedCrimesPerBlock = casesPerBlockRDD.sortBy(lambda x: x[1])
# Collect
result = sortedCrimesPerBlock.collect()[-10:] # Get the 10 with highest number

with open("dimitrov_exercise2_part1_output.txt", 'w') as output:
    for row in result:
        output.write(str(row) + '\n')

# COMMENTS:
# Result i sin dimitrov_exercise2_part1_output.txt
# ('064XX S DR MARTIN LUTHER KING JR DR', 571)
# ('076XX S CICERO AVE', 572)
# ('100XX W OHARE ST', 652)
# ('006XX N MICHIGAN AVE', 665)
# ('011XX S CANAL ST', 754)
# ('0000X S STATE ST', 898)
# ('0000X N STATE ST', 914)
# ('0000X W TERMINAL ST', 942)
# ('008XX N MICHIGAN AVE', 1071)
# ('001XX N STATE ST', 2290)