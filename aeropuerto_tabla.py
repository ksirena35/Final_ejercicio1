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
informe_2021 = spark.read.options(header="true", sep=";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
informe_202206 = spark.read.options(header="true", sep=";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")

##verifico que el schema sea el mismo para poder realizar la union de los dos dataframe.

informe_2021.schema == informe_202206.schema

##relizo la union de los datos del ano 2021 y 2022.

informes = informe_2021.union(informe_202206)

## Realizo las transformaciones pedidas en el  punto 4.

aeropuerto_tabla = (
    informes
    .withColumn("fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))
    .withColumnRenamed("Hora UTC", "horaUTC")
    .withColumnRenamed("Clase de Vuelo (todos los vuelos)", "clase_de_vuelo")
    .withColumnRenamed("Clasificación Vuelo", "clasificacion_de_vuelo")
    .withColumnRenamed("Tipo de Movimiento", "tipo_de_movimiento")
    .withColumnRenamed("Aeropuerto", "aeropuerto")
    .withColumnRenamed("Origen / Destino", "origen_destino")
    .withColumnRenamed("Aerolinea Nombre", "aerolinea_nombre")
    .withColumnRenamed("Aeronave", "aeronave")
    .withColumn("pasajeros", F.col("Pasajeros").cast(T.IntegerType()))
    .drop("Calidad dato")
)

aeropuerto_tabla = (
    aeropuerto_tabla
    .withColumn("clasificacion_de_vuelo", F.when(F.col("clasificacion_de_vuelo") == "Doméstico", "Domestico")
                                                .otherwise(F.col("clasificacion_de_vuelo")))
    .filter(F.col("clasificacion_de_vuelo") == "Domestico")
    .na.fill({"pasajeros": 0})
)

##Insert de la tabla aeropuerto_tabla en la base de datos Viajes  previamente creada en Hive.
aeropuerto_tabla.write.mode("overwrite").format("hive").saveAsTable("viajes.aeropuerto_tabla")



