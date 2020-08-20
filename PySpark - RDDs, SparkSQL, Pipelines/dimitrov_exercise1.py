# !chmod 755 exercise1_run
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split # Used for splitting up the Date column
import pandas as pd

sc = SparkContext()
sqlcontext = SQLContext(sc)

# Loading Data
# csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/sample_crime_data.csv'
csv_path = 'hdfs://wolf.analytics.private/user/ktd5131/data/crime/Crimes_-_2001_to_present.csv'
df_csv = sqlcontext.read.csv(csv_path, header = True)

# Creating a new column which contains the month extracted from the date column
split_date = split(df_csv['Date'], '/')
df_csv = df_csv.withColumn('Month', split_date.getItem(0))

# Performing the required aggregation over Month & Year
month_year_count = df_csv.groupBy(['Month', 'Year']).count()

# Aggregating over Year to get average
monthly_crime_avg = month_year_count.groupBy('Month').avg('count').orderBy('Month')

# Saving to Pandas DataFrame
pd_df = monthly_crime_avg.toPandas()

# Generating Histogram
fig = pd_df.plot(x='Month', y='avg(count)', kind='bar').get_figure()
# Saving histogram
fig.savefig('dimitrov_histogram.png')

# COMMENTS:
# Looking at the histogram, it appears there are ~20-25% more crimes on average during the summer months
# as opposed to the winter months. This makes sense - although it's not clear if there are fewer crimes
# or fewer crimes are recorded/detected.


