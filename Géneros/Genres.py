import sys
from pyspark import SparkContext, SparkConf
from os import remove
from shutil import rmtree
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark.sql import functions
from pyspark.sql.functions import split, explode, col, concat_ws, avg, sum


conf = SparkConf().setMaster('local').setAppName('GenreAnalysis')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
df = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('/home/ubuntu/steam.csv')

def w(x):
    print(x)

def genreMediaTime(df):
    g = df.withColumn('steamspy_tags',explode(split('steamspy_tags',';')))
    g = g.select('steamspy_tags', 'average_playtime')
    g = g.withColumn('average_playtime', df['average_playtime'].cast("float"))
    g = g.groupBy('steamspy_tags')
    g = g.agg(avg('average_playtime'))

    try:
        remove("/home/ubuntu/GENRESMEDIATIME")
    except Exception:
        pass
    g.coalesce(1).write.option("inferSchema","true").csv("GenresMediaTime",header='true')
    return g

def genreMediaPrice(df):
    g = df.withColumn('steamspy_tags',explode(split('steamspy_tags',';')))
    g = g.select('steamspy_tags', 'price')
    g = g.withColumn('price', df['price'].cast("float"))
    g = g.groupBy('steamspy_tags')
    g = g.agg(avg('price'))

    try:
        remove("/home/ubuntu/GENRESMEDIAPRICE")
    except Exception:
        pass
    g.coalesce(1).write.option("inferSchema","true").csv("GENRESMEDIAPRICE",header='true')
    return g

def countGenres(df):
    g = df.withColumn('steamspy_tags',explode(split('steamspy_tags',';')))
    g = g.select('steamspy_tags')

    lines = g.rdd.map(lambda line: (line[0], 1))
    result = lines.reduceByKey(lambda a, b: a+b)
    deptcolumns = ["genre", "count"]
    result = result.toDF(deptcolumns)
    result.show()
    try:
        remove("/home/ubuntu/GENRESCOUNT")
    except Exception:
        pass
    g.coalesce(1).write.option("inferSchema","true").csv("GENRESCOUNT",header='true')
    return result

def RecommendationGenres(df):
    g = df.withColumn('steamspy_tags',explode(split('steamspy_tags',';')))
    g = g.select('steamspy_tags', 'positive_ratings', 'negative_ratings')
    g = g.groupBy('steamspy_tags')
    g = g.agg(sum('positive_ratings'), sum('negative_ratings'))
    newg = g.withColumn('percentage', g['sum(positive_ratings)'] /(g['sum(positive_ratings)'] + g['sum(negative_ratings)']))
    newg.show()

    try:
        remove("/home/ubuntu/GENRESRECOMMENDATIONPERCENTAGE")
    except Exception:
        pass
    newg.coalesce(1).write.option("inferSchema","true").csv("GENRERECOMMENDATIONS",header='true')
    return newg

def getMax(df, key_col, K):
    window = Window.orderBy(functions.col(key_col).desc())
    return (df
            .withColumn("rank", functions.rank().over(window))
            .filter(functions.col('rank') <= K)
            .drop('rank'))

def getMostRecommended(df, minimum):
    df = RecommendationGenres(df)
    df = df.filter(df['percentage'] < minimum)
    df = df.select('steamspy_tags', 'percentage')
    df = getMax(df, 'percentage', 10)
    return df
