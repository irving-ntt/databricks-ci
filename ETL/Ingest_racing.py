# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_smartdata")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"/Volumes/{catalogo}/{container}/datasets/races.csv"

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
            .option("header", True) \
            .schema(races_schema) \
            .csv(ruta)

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), 
                                                   col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),
                                                   col('name'), col('ingestion_date'))

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').saveAsTable(f'{catalogo}.{esquema}.races')
