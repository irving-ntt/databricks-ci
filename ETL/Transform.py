# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

catalogo = "adbsmartdata_prod"
esquema_source = "bronze"
esquema_sink = "silver"

# COMMAND ----------

def altitud_categoria(altitude):
    if altitude < 100:
        return "Baja"
    elif 100 <= altitude < 200:
        return "Media"
    else:
        return "Alta"

# COMMAND ----------

altitud_udf = F.udf(altitud_categoria, StringType())

# COMMAND ----------

df_circuit = spark.table(f"{catalogo}.{esquema_source}.circuits")
df_races = spark.table(f"{catalogo}.{esquema_source}.races").withColumnRenamed("name","name_race")

# COMMAND ----------

df_circuit = df_circuit.dropna(how="all")\
                        .filter((col("circuit_id").isNotNull()) | (col("circuit_ref")).isNotNull())

df_races = df_races.dropna(how="all")\
                    .filter((col("race_id").isNotNull()) | (col("circuit_id")).isNotNull())

# COMMAND ----------

df_circuit = df_circuit.withColumn("altitude_category", altitud_udf("altitude"))

# COMMAND ----------

df_joined = df_races.alias("x").join(df_circuit.alias("y"), col("x.circuit_id") == col("y.circuit_id"), "inner")

# COMMAND ----------

df_filtered_sorted = df_joined.filter(df_races.race_year > 1978).orderBy("race_id")

# COMMAND ----------

df_filtered_sorted = df_filtered_sorted.withColumn(
    "years_diferences", 
    F.year(F.current_date()) - F.col("race_year")
)

# COMMAND ----------

df_aggregated = df_filtered_sorted.groupBy("country", "circuit_ref").agg(
    F.count("race_id").alias("num_races_1978")
)

# COMMAND ----------

df_with_latitude_diff = df_filtered_sorted.withColumn(
    "lat_diff", 
    F.abs(df_filtered_sorted["latitude"] - df_filtered_sorted["latitude"].alias("latitude")).cast(IntegerType())
)

# COMMAND ----------

df_updated = df_with_latitude_diff.select("*",
                                    when(col("circuit_ref").isin("albert_park", "sepang", "bahrain"), lit("Internacional")).otherwise("Local").alias("race_type"),
                                    when((col("latitude")>-10) & (col("latitude")<10),lit("Cerca del ecuador")).otherwise(lit("Lejos del ecuador")).alias("near_equator")).drop(col("y.circuit_id"),col("y.ingestion_date"))

# COMMAND ----------

df_updated.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.circuits_transformed")
