from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import sql
import string
import datetime

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

def cleanDF(df, column):

    ascii_udf = udf(ascii_ignore)
    df = df.withColumn(column, ascii_udf(column))
    df = df.filter(df[column] != "")

    return df


conf = SparkConf().setMaster('local').setAppName('DesarrolladorHorasJugadas')

sc= SparkContext(conf = conf)

spark = SparkSession(sc)

df = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('steam.csv')

df = cleanDF(df,'developer')

df = df.withColumn('average_playtime', df['average_playtime'].cast("float"))

df= df.select('developer','average_playtime').groupBy('developer').avg('average_playtime')

df = df.filter(col("avg(average_playtime)").cast("float"))

df.coalesce(1).write.option("inferSchema","true").csv("DeveloperMediaPlayTime",header='true')


