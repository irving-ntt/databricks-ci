dbutils.widgets.removeAll()

# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_circuits_transformed = spark.table(f"{catalogo}.{esquema_source}.circuits_transformed")

# COMMAND ----------

df_transformed = df_circuits_transformed.groupBy(col("race_year")).agg(
                                                     count(col("location")).alias("conteo"),
                                                     max(col("altitude")).alias("max_altitude"),
                                                     min(col("altitude")).alias("min_altitude"),
                                                     max(col("country")).alias("country"),
                                                     max(col("race_type")).alias("race_type"),
                                                     max(col("near_equator")).alias("near_equator")
                                                     ).orderBy(col("race_year").desc())

# COMMAND ----------

df_transformed.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_raced_partitioned")
