# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0100_EXT_TRN_FOV
# MAGIC
# MAGIC **Descripción:** Job paralelo que extrae la información de los registros que fueron rechazados por Procesar de transferencias de FOVISSSTE.
# MAGIC
# MAGIC **Subetapa:** Extracción de rechazos para reverso de movimientos
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_TRANS_FOVISSSTE (Oracle - transferencias)
# MAGIC - CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA (Oracle - matriz convivencia)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - RESULTADO_RECH_PROCESAR_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **BD_100_TL_PRO_TRANS_FOVISSSTE**: Extraer registros rechazados desde Oracle con LEFT JOIN
# MAGIC 2. **DS_200_RECH_PROCESAR**: Escribir resultado en tabla Delta de Databricks
# MAGIC 3. El dataset resultante será consumido por jobs subsecuentes en la secuencia
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Secuencia Completa (JQ_PATRIF_REVMOV_0100_TRNS_FOV)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/JQ_PATRIF_REVMOV_0100_TRNS_FOV.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC ### Job Paralelo (JP_PATRIF_REVMOV_0100_EXT_TRN_FO)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/JP_PATRIF_REVMOV_0100_EXT_TRN_FO.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC ## Stage BD_100_TL_PRO_TRANS_FOVISSSTE
# MAGIC
# MAGIC **Type:** Oracle Connector PX (Source)
# MAGIC **Version:** 11g
# MAGIC **Description:** Extrae registros de transferencias FOVISSSTE rechazados con LEFT JOIN a matriz de convivencia
# MAGIC
# MAGIC **Filtros aplicados:**
# MAGIC - FTC_FOLIO = p_SR_FOLIO
# MAGIC - FCN_ID_SUBPROCESO = p_SR_SUBPROCESO
# MAGIC - FTC_ESTATUS = 0
# MAGIC - FTC_TIPO_ARCH = 09
# MAGIC - FTN_MOTIVO_RECHAZO = 99
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage BD_100_TL_PRO_TRANS_FOVISSSTE
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/FOVISSSTE/Notebooks/assets/BD_100_TL_PRO_TRANS_FOVISSSTE.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,BD_100_TL_PRO_TRANS_FOVISSSTE + DS_200_RECH_PROCESAR
# Extraer registros de transferencias FOVISSSTE rechazados por procesar
statement_extract = query.get_statement(
    "NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_TRANS_FOVISSSTE=conf.TL_PRO_TRANS_FOVISSSTE,
    TL_MATRIZ_CONVIV=conf.TL_CRN_MATRIZ_CONV,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_PROCESO=params.sr_proceso,
)

# Extraer datos y escribir directamente en Delta
db.write_delta(
    f"RESULTADO_RECH_PROCESAR_{params.sr_folio}",
    db.read_data("default", statement_extract),
    "overwrite",
)

logger.info(f"✅ Registros rechazados FOVISSSTE extraídos - Folio: {params.sr_folio}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Job Completado
# MAGIC
# MAGIC La tabla Delta `RESULTADO_RECH_PROCESAR_{sr_folio}` está lista para ser consumida por los siguientes jobs de la secuencia.

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
