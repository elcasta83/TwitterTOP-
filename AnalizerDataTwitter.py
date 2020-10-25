#!/usr/bin/python
 # -*- coding: utf-8 -*- 

from pyspark import SparkContext
import re
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
        	return sum(new_values) + (total_sum or 0)
        	
def get_sql_context_instance(spark_context):
        	if ('sqlContextSingletonInstance' not in globals()):
        		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
        	return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):
        print("----------- %s -----------" % str(time))
        try:
        	# obtén el contexto spark sql singleton desde el contexto actual
        	sql_context = get_sql_context_instance(rdd.context)
        	# convierte el RDD a Row RDD
        	row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        	# crea un DF desde el Row RDD
        	hashtags_df = sql_context.createDataFrame(row_rdd)
        	# Registra el marco de data como tabla
        	hashtags_df.registerTempTable("hashtags")
        	# obtén los 10 mejores hashtags de la tabla utilizando SQL e imprímelos
        	hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        	hashtag_counts_df.show()
        	# llama a este método para preparar los 10 mejores hashtags DF y envíalos
        	send_df_to_dashboard(hashtag_counts_df)
        except:
        	pass
        	e = sys.exc_info()[0]
        	print("Error????: %s" % e)

def send_df_to_dashboard(df):
        	# extrae los hashtags del marco de data y conviértelos en una matriz
        	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
        	# extrae las cuentas del marco de data y conviértelos en una matriz
        	tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
        	# inicia y envía la data a través de la API REST
        	url = 'http://localhost:5001/updateData'
        	request_data = {'label': str(top_tags), 'data': str(tags_count)}
        	response = requests.post(url, data=request_data)


# crea una configuración spark
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# crea un contexto spark con la configuración anterior
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# crea el Contexto Streaming desde el contexto spark visto arriba con intervalo de 2 segundos
ssc = StreamingContext(sc, 10)
# establece un punto de control para permitir la recuperación de RDD
ssc.checkpoint("checkpoint_TwitterApp")
# lee data del puerto 9009
print ("Comienza a leer el puerto")
dataStream = ssc.socketTextStream("localhost",9990)
# divide cada Tweet en palabras
words = dataStream.flatMap(lambda line: line.split(" "))
# filtra las palabras para obtener solo hashtags, luego mapea cada hashtag para que sea un par de (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# agrega la cuenta de cada hashtag a su última cuenta
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# procesa cada RDD generado en cada intervalo
tags_totals.foreachRDD(process_rdd)
# comienza la computación de streaming
ssc.start()
# espera que la transmisión termine
ssc.awaitTermination()
