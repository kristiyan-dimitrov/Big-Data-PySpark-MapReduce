from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors
from operator import add
import pandas as pd
import numpy as np

# Helper Functions
# Calculating correlation b/w two beats
def corr_pair(tpl):
    return  (tpl[0][0], (tpl[1][0],  np.corrcoef(tpl[0][1], tpl[1][1])[0][1]) )
# Returning only combination with higher correlation for a specific beat
def higher_corr(tpl0, tpl1):
    if tpl0[1] > tpl1[1]:
        return (tpl0)
    else:
        return (tpl1)

sc = SparkContext()
sqlcontext = SQLContext(sc)
sc.setLogLevel('Error')

# PART 2
# Loading Data
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/sample_crime_data.csv'
rddd = sc.textFile(csv_path)
# Remove irrelevant data
filteredRDD = rddd.filter(lambda x: x.split(",")[17] in {'2016', '2017', '2018', '2019', '2020'})
# GroupBy Beat & Year
beatAndYear = filteredRDD.map(lambda x: ( (x.split(",")[10], x.split(",")[17]) ,1))
# Calculate # of crimes per beat/year combo
beatAndYearCount = beatAndYear.reduceByKey(add)
# Prepare for GroupBy Beat
beatYears = beatAndYearCount.map(lambda x: (x[0][0],(x[0][1],x[1])))
# Group By Beat; Here are 2 example values: ('1624', [('2019', 3), ('2020', 1), ('2016', 9), ('2017', 4)]), ('1125', [('2016', 2), ('2017', 3), ('2019', 1)])]
beatYearsGrouped = beatYears.groupByKey().mapValues(list)
# Ordering the yearly values; Sample Output: ('1611', [('2016', 4), ('2017', 3), ('2018', 2), ('2020', 1)])  ... Issue is not all 5 years are present :O
beatYearsGroupedSorted = beatYearsGrouped.map(lambda x: (x[0], sorted(x[1], key = lambda tup: tup[0])))
# Extracting only the counts into a list; Example output: ('2233', [4, 4, 4, 7]), ('0812', [3, 4, 2, 4]), ...
beatYearsGroupedList = beatYearsGroupedSorted.map(lambda tpl: (tpl[0], [el[1] for el in tpl[1]]))
# Removing any beats that don't have measurements for all 5 past years
beatYearsGroupedFiltered = beatYearsGroupedList.filter(lambda tpl: len(tpl[1])==5)
## Calculating the cartisian product of my RDD on itself. Filtering using "<" to avoid duplicates
cartesianProduct = beatYearsGroupedFiltered.cartesian(beatYearsGroupedFiltered).filter(lambda tpl: int(tpl[0][0]) < int(tpl[1][0]))
# Generating Final List
finalList = cartesianProduct.map(corr_pair).reduceByKey(higher_corr).sortBy(lambda el: el[1][1], ascending = False).collect()

with open("dimitrov_exercise2_part2_output.txt", 'w') as output:
    for row in finalList:
        output.write(str(row) + '\n')


# Results are in dimitrov_exercise2_part2_output.txt
# ('1135', ('1231', 0.9999779623966597))
# ('0714', ('0825', 0.9999495993367986))

# ANSWER:
# The first two: 1135 & 1231 are almost next to each other, but the next two: 0714 and 0825 are right next to each other. 
# Therefore 0714 and 0825  is the answer.

# COMMENTS
# The crux of the above operations are the final two manipulations: the Cartisian product and the reduceByKey








