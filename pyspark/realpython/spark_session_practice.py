import pyspark as ps
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()

df = spark.read.csv('Binance_BTCUSDT_minute.csv', header=True)

print(type(df))

df.show(5)
