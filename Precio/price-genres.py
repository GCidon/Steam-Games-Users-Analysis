from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,dayofmonth
from pyspark.sql import functions
from pyspark.sql import SQLContext
from pyspark import sql
import string
import datetime

conf = SparkConf().setMaster('local').setAppName('wordcount')
sc = SparkContext(conf = conf)

spark = SparkSession(sc)

rdd = sc.textFile("steam.csv")
rdd = rdd.filter(lambda line : '"' not in line)
rdd = rdd.map(lambda x: x.split(",")[0:18])
row= rdd.first()
dataset= rdd.filter(lambda line: line!=row)
df = dataset.toDF()

df = df.select('_10','_18')

df = df.withColumnRenamed('_10', 'Genero')

df = df.withColumnRenamed('_18','Precio')

df = df.withColumn('Precio',df['Precio'].cast("float")).groupBy('Genero').avg('Precio').orderBy('Genero')

df.coalesce(1).write.option("inferSchema","true").csv("Precio-genero",header='true')


