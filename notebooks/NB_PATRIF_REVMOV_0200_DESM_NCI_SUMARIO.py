# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0200_DESM_NCI_SUMARIO (Salida SUMARIO)
# MAGIC
# MAGIC **Descripción:** Notebook de salida SUMARIO que genera el registro de sumario para archivo Integrity.
# MAGIC
# MAGIC **Subetapa:** Generación de registro sumario para Integrity
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:** NA (genera registro fijo)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - RESULTADO_SUMARIO_INTEGRITY_{sr_folio} (dataset sufijo 03)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Notebook:
# MAGIC 1. Generar registro de sumario con formato fijo de 128 caracteres
# MAGIC 2. Guardar en tabla Delta (equivalente a dataset sufijo 03)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Secuencia Completa
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1743575643804651?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_subetapa": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_subproceso": str,
        "sr_fec_liq": str,
        "sr_fec_acc": str,
        "sr_proceso": str,
    }
)
params.validate()

# Cargar configuración de entorno
conf = ConfManager()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 SALIDA SUMARIO: GENERACIÓN DE REGISTRO SUMARIO
# MAGIC
# MAGIC **Formato del Registro Sumario (128 caracteres):**
# MAGIC - Posición 1-2: '03' (tipo de registro sumario)
# MAGIC - Posición 3-37: '0' repetido 35 veces
# MAGIC - Posición 38-128: Espacios (91 caracteres)
# MAGIC
# MAGIC **Nota:** Este registro es único por archivo y tiene formato fijo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: DS_900_SUMARIO
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_007.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Generar Archivo Sumario
statement_sumario = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql",
)

db.write_delta(
    f"RESULTADO_SUMARIO_INTEGRITY_{params.sr_folio}",
    db.sql_delta(statement_sumario),
    "overwrite",
)

logger.info("Registro sumario Integrity generado exitosamente")

# COMMAND ----------

# DBTITLE 2,DEBUG
if conf.debug:
    display(db.read_delta(f"RESULTADO_SUMARIO_INTEGRITY_{params.sr_folio}"))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
