from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window
from pyspark.sql.functions import udf, col
from os import remove
import string
import sys

def getMax(df, key_col, K):
    window = Window.orderBy(functions.col(key_col).desc())
    return (df
            .withColumn("rank", functions.rank().over(window))
            .filter(functions.col('rank') <= K)
            .drop('rank'))

def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')

def cleanDF(df, column):

    ascii_udf = udf(ascii_ignore)
    df = df.withColumn(column, ascii_udf(column))
    df = df.filter(df[column] != "")

    return df

def basicInfo(gameInfo):

    genres = str(gameInfo.select('steamspy_tags').collect()[0][0])

    gameId = str(gameInfo.select('appid').collect()[0][0])

    return gameId, genres

#Devuelve los juegos rankeados en base al numero de horas que se ha jugado
def mostPlayed(df):

    splitted = functions.split(df['owners'], '-')
    df = df.withColumn('min_playtime', df['average_playtime'].cast("long") * splitted.getItem(0).cast("long"))
    df = df.withColumn('max_playtime', df['average_playtime'].cast("long") * splitted.getItem(1).cast("long"))

    df = df.withColumn('avg_total_playtime', (df['min_playtime'].cast("long") + df['max_playtime'].cast("long")) / 2)
    
    window = Window.orderBy(functions.col('avg_total_playtime').desc())
    df = df.withColumn("rank_playtime", functions.rank().over(window))

    return df


def rating (df):

    df = df.withColumn('rating_percentage', 100 * df['positive_ratings'].cast("int") /(df['positive_ratings'].cast("int") + df['negative_ratings'].cast("int")))
    df = df.withColumn('rating_percentage', df['rating_percentage'].cast("float"))

    return df 

#Devuelve los juegos recomendados en base a otro juego 
def recommended(df, genres, gameId):

    #Primero filtramos aquellos juegos que se consideran 'malos' (rating < 50) y al propio juego (no se va a recomendar a si mismo)
    recommended = df.filter(df['rating_percentage'] >= 50).filter(df['appid'] != gameId)

    #Filtramos tambien los juegos que no suman mas de 50 valoraciones, pues los consideramos muy poco "populares" como 
    #para ser tenidos en cuenta
    recommended = recommended.filter(recommended['positive_ratings'].cast("int") + recommended['negative_ratings'].cast("int") > 50)

    #Filtramos aquellos juegos que no tienen ningun genero en comun con nuestro juego, y los restantes priorizamos en base a la cantidad
    #de generos que tienen en comun con nuestro juego. Es importante tener en cuenta que los juegos ya estan ordenados en base a 
    #su posicion en el ranking de tiempo de juego, por lo cual en caso de que dos juegos tengan los mismos generos en comun, se da 
    #preferencia al juego que esta mejor posicionado en dicho ranking
    recommended = recommended.withColumn('steamspy_tags',functions.explode(functions.split('steamspy_tags',';')))
    genres = genres.split(';')
    notRecommended = recommended

    for genre in genres:
        notRecommended = notRecommended.filter(functions.lower(notRecommended['steamspy_tags']) != genre.lower()) 
    
    recommended = recommended.subtract(notRecommended)
    recommended = recommended.groupBy('name', 'rank_playtime').count()

    recommended = getMax(recommended, 'count', 20).select('name', 'rank_playtime', col('count').alias('common_genres'))

    return recommended


game = str(sys.argv[1]).lower()

conf = SparkConf().setMaster('local').setAppName('Game Info')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
dataFrame = spark.read.format("csv").options(header=True, inferSchema=True, sep=",").load('/home/ubuntu/steam.csv')

dataFrame = cleanDF(dataFrame, 'name')

gameInfo = dataFrame.filter(functions.lower(dataFrame['name']) == game)

if(gameInfo.count() == 0):
    print("El juego que has solicitado no esta registrado!")

else:

    gameId, genres = basicInfo(gameInfo)

    dataFrame = rating(dataFrame)
    dataFrame = mostPlayed(dataFrame)

    rec = recommended(dataFrame, genres, gameId)

    rec.show()