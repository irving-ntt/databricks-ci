# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0200_DESM_NCI_DETALLE (Salida DETALLE)
# MAGIC
# MAGIC **Descripción:** Notebook de salida DETALLE que genera el archivo de detalle para Integrity (elimina duplicados y formatea registros).
# MAGIC
# MAGIC **Subetapa:** Generación de archivo detalle para Integrity
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - TEMP_TR500_CAMPOS_{sr_folio} (Delta - con variables calculadas)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - RESULTADO_DETALLE_INTEGRITY_{sr_folio} (dataset sufijo 02)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Notebook:
# MAGIC 1. Leer tabla Delta con variables calculadas
# MAGIC 2. Eliminar duplicados por FTN_NUM_CTA_AFORE (mantener primer registro)
# MAGIC 3. Generar campo DETALLE con formato específico de 128 caracteres
# MAGIC 4. Guardar en tabla Delta (equivalente a dataset sufijo 02)
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
# MAGIC ## 🔄 SALIDA DETALLE: GENERACIÓN DE ARCHIVO INTEGRITY
# MAGIC
# MAGIC **Formato del Registro Detalle (128 caracteres):**
# MAGIC - Posición 1-2: '02' (tipo de registro)
# MAGIC - Posición 3-12: Número de cuenta (10 caracteres)
# MAGIC - Posición 13-16: Código proceso Integrity (4 caracteres)
# MAGIC - Posición 17-20: Código subproceso Integrity (4 caracteres)
# MAGIC - Posición 21-31: NSS del trabajador (11 caracteres)
# MAGIC - Posición 32-49: CURP del trabajador (18 caracteres)
# MAGIC - Posición 50-128: Espacios (79 caracteres)
# MAGIC
# MAGIC **Eliminación de Duplicados:**
# MAGIC - Se eliminan registros duplicados por número de cuenta
# MAGIC - Se mantiene el primer registro según el CONTADOR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: Remove_Duplicates_88 + DS_800_DETALLE
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1743575643804734?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_004.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1743575643804737?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_005.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1743575643804736?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_009.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Generar Archivo Detalle
statement_detalle = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"RESULTADO_DETALLE_INTEGRITY_{params.sr_folio}",
    db.sql_delta(statement_detalle),
    "overwrite",
)

logger.info("Archivo detalle Integrity generado exitosamente")

# COMMAND ----------

# DBTITLE 2,DEBUG
if conf.debug:
    display(db.read_delta(f"RESULTADO_DETALLE_INTEGRITY_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ NOTEBOOK DETALLE COMPLETADO
# MAGIC
# MAGIC La tabla Delta `RESULTADO_DETALLE_INTEGRITY_{sr_folio}` contiene:
# MAGIC - Registros únicos por número de cuenta (sin duplicados)
# MAGIC - Campo DETALLE formateado en 128 caracteres
# MAGIC - ID_REGISTRO = '02'
# MAGIC - Listo para ser consumido por el siguiente job de la secuencia

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
