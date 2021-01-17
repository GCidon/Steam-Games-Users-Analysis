from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import sql
import string
import datetime


conf = SparkConf().setMaster('local').setAppName('VentasHorasJugadas')

sc= SparkContext(conf = conf)

spark = SparkSession(sc)

df = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')


df = df.withColumn('average_playtime', df['average_playtime'].cast("float"))

df = df.withColumn('owners', df['owners'].cast("string"))

df = df.filter(df['owners'].contains("-"))

df= df.select('owners', 'average_playtime').groupBy('owners').avg('average_playtime').orderBy('avg(average_playtime)')

df.coalesce(1).write.option("inferSchema","true").csv("SalesMediaPlayTime",header='true')






