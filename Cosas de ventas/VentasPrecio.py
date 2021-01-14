from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark import sql
import string
import datetime

conf = SparkConf().setMaster('local').setAppName('VentasPrecio')

sc= SparkContext(conf = conf)

spark = SparkSession(sc)

df = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')

df = df.withColumn('price', df['price'].cast("float"))

df = df.withColumn('owners', df['owners'].cast("string"))

df = df.filter(df['owners'].contains("-"))

df= df.select('owners', 'price').groupBy('owners').avg('price').orderBy('avg(price)')

df.coalesce(1).write.option("inferSchema","true").csv("SalesPrice",header='true')
