from pyspark import SparkContext
from pyspark.sql import SQLContext
print("Hello!")

if __name__ == "__main__":
    sc = SparkContext(appName="myApp")
    sc.setLogLevel('ERROR')
    sqlcontext = SQLContext(sc)
    print("Reading data")
    textFile = sqlcontext.read.csv("s3://msia423-homework3-ktd5131/sample.csv", header = True)
    print("read data!")
    # counts = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    samples = textFile.sample(.01, False, 42)
    print("took sample!")
    df = samples.toPandas()
    print("Made pandas")
    df.to_csv('s3://msia423-homework3-ktd5131/output.csv', index = False)
    print("wrote fie")

    sc.stop()
# samples.saveAsTextFile("hdfs://wolf.analytics.private/user/mtl8754/example/wc_spark/")
