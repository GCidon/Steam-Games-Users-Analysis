from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import sql
import string
import datetime
import pandas as pd

conf = SparkConf().setMaster('local').setAppName('VentasRatings')

sc= SparkContext(conf = conf)

spark = SparkSession(sc)

dfpositive = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')

dfnegative = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')

dfpositive = dfpositive.withColumn('positive_ratings', dfpositive['positive_ratings'].cast("float"))

dfnegative = dfnegative.withColumn('negative_ratings', dfnegative['negative_ratings'].cast("float"))

dfpositive = dfpositive.filter(dfpositive['owners'].contains("-"))

dfnegative = dfnegative.filter(dfnegative['owners'].contains("-"))

dfpositive= dfpositive.select('owners', 'positive_ratings').groupBy('owners').avg('positive_ratings').orderBy('avg(positive_ratings)')

dfnegative= dfnegative.select('owners', 'negative_ratings').groupBy('owners').avg('negative_ratings').orderBy('avg(negative_ratings)')

dfpositive = dfpositive.withColumnRenamed('owners', 'OWN')

dfrango = dfpositive.join(dfnegative, dfpositive.OWN == dfnegative.owners, how = 'full')

dfrango = dfrango.withColumn('rango',dfrango['avg(negative_ratings)']/dfrango['avg(positive_ratings)'])

dfrango= dfrango.select('owners','rango').orderBy('rango')

dfrango.coalesce(1).write.option("inferSchema","true").csv("VentasRatings",header='true')


