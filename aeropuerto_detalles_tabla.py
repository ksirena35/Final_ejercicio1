#!/usr/bin/env python3

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

from pyspark.sql import functions as F
from pyspark.sql import types as T
##leo el archivo csv ingestado  desde HDFS y lo cargo en un dataframe.

aeropuertos_detalle = spark.read.options(header="true", sep=";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

## aplico las transformaciones del punto 4.


aeropuerto_detalles_tabla = (
    aeropuertos_detalle
    .withColumnRenamed("local", "aeropuerto")
    .withColumnRenamed("oaci", "oac")
    .withColumn("elev", aeropuertos_detalle["elev"].cast(T.FloatType()))
    .withColumn("distancia_ref", F.col("distancia_ref").cast(T.FloatType()))
    .drop("inhab", "fir")
    .na.fill({"distancia_ref": 0})
)

aeropuerto_detalles_tabla = (
    aeropuerto_detalles_tabla
    .na.fill({"distancia_ref": 0})
)



##Insert de la tabla aeropuerto_detalles_tabla en la base de datos Viajes  previamente creada en Hive.

aeropuerto_detalles_tabla.write.mode("append").format("hive").saveAsTable("viajes.aeropuerto_detalles_tabla")