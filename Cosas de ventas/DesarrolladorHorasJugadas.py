from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark import sql
import string
import datetime
import re
conf = SparkConf().setMaster('local').setAppName('DesarrolladorHorasJugadas')

sc= SparkContext(conf = conf)

spark = SparkSession(sc)

df = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')

df = df.withColumn('average_playtime', df['average_playtime'].cast("float"))

df= df.select('developer','average_playtime').groupBy('developer').avg('average_playtime')

df.coalesce(1).write.option("inferSchema","true").csv("DeveloperMediaPlayTime",header='true')



