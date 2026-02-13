# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_smartdata")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"/Volumes/{catalogo}/{container}/datasets/circuits.csv"

# COMMAND ----------

df_circuits = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_circuits_final = spark.read\
.option('header', True)\
.schema(circuits_schema)\
.csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
circuits_selected_df = df_circuits_final.select(col("circuitId"), 
                                                col("circuitRef"), 
                                                col("name"), col("location"), 
                                                col("country"), 
                                                col("lat"), 
                                                col("lng"), 
                                                col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                            .withColumnRenamed("circuitRef", "circuit_ref") \
                                            .withColumnRenamed("lat", "latitude") \
                                            .withColumnRenamed("lng", "longitude") \
                                            .withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.circuits")
