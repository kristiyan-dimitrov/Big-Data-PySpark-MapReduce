""" 
I will check by REGION how average number of crimes per year have changed for both mayors
By region, I mean the first two digits of the beat as an identifier

NOTE FROM THE FUTURE: Only later did I realize that the 'District' column corresponds precisely to this
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np

# Helper Functions
def whatMayor(year):
    if not year: # If year is empty string '' --> Unknown
        return "Unknown"
    
    if type(year) == str:
        if len(year) == 4:
            year = int(year)
        else: # If year value is invalid --> Unknown
            return "Unknown"
    # Mayor Logic: Emanuel b/w 2011-2019; Daly 1989-2010               
    if (year > 2010) and (year < 2020):
        return "Emanuel"
    elif (year>1988) and (year <= 2010):
        return "Daly"
    else:
        return "Other"

# # Used this code to confirm the count of distinct years in the data is 20
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
# rddd = sc.textFile(csv_path)
# rdddYear = rddd.map(lambda x: x.split(",")[18])
# rdddYearCleaned = rdddYear.filter(lambda x: (x != 'Year') and (x != '') and (len(x)==4))
# rdddDistinct = rdddYearCleaned.distinct()
# Count = rdddDistinct.count()

sc = SparkContext()
sc.setLogLevel('Error')


# PART 2
# Loading Data
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/sample_crime_data.csv'
rddd = sc.textFile(csv_path)
header = rddd.first()
rdddNoHeader = rddd.filter(lambda x: x != header)
# Calculating the number of cases per mayor per region NOTE FROM THE FUTURE: Only later did I realize that the 'District' column corresponds precisely to this
mayorByBeat = rdddNoHeader.map(lambda row: ((row.split(",")[10][:2], whatMayor(row.split(",")[17])), row.split(",")[1])  )
crimeCount = mayorByBeat.countByKey() # This is the number of crimes per region

# Note that crimeCount is now a regular dictionary (not an RDD). 
# I found out we're not supposed to use pandas after I had already used it to format the data appropriately
# Due to time constaints, I won't try to format the data into a regular Python dictionary (because it still wouldn't be an RDD)
# Also, I haven't included scipy in my zipped environment, but have placed the commands for performing the t-test at the bottom 

# Format the data in a DataFrame. just so they are easier to look at
df = pd.DataFrame(columns=['Daly', 'Emanuel'])

# Populate the DataFrame with the data from the dictionary
for key, value in crimeCount.items():
    if key[1] == "Emanuel":
        df.loc[key[0], key[1]] = value/9 # 9 years for Emanuel in the data
    elif key[1] == "Daly":
        df.loc[key[0], key[1]] = value/11 # 11 years for Daly in the data

df = df.sort_index()
df.to_csv("dimitrov_exercise2_part3_output.csv")

# RESULTS are in dimitrov_exercise2_part3_output.csv
# Sample output:

# Beat    Daly                    Emanuel
# 1   12769.454545454500  12977.555555555600
# 2   12478.636363636400  11124.666666666700
# 3   20062.454545454500  13992.555555555600
# 4   21295.0             16109.111111111100
# 5   16948.363636363600  12439.333333333300

# COMMENTS:
# Looking at the numbers, it definitely appears that the average number of crimes
# per year was higher during Daly's tenure as opposed to during Emanuel's.

# For making a t-test on the two columns I would do:
# from scipy.stats import ttest_ind
# t_statistic, p_value = ttest_ind(df['Daly'], df['Emanuel'])










