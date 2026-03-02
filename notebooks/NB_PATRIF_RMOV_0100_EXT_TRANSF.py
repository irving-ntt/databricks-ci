# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_RMOV_0100_EXT_TRANSF
# MAGIC
# MAGIC **Descripción:** Job paralelo que extrae la información de los registros que fueron rechazados por Procesar según corresponda el subproceso (TA, UG, AG) de transferencias de INFONAVIT.
# MAGIC
# MAGIC **Subetapa:** Extracción de rechazos para reverso de movimientos
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_TRANS_INFONA (Oracle - transferencias)
# MAGIC - CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA (Oracle - matriz convivencia)
# MAGIC - CIERREN.TTAFOGRAL_IND_CTA_INDV (Oracle - indicadores cuenta)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - RESULTADO_RECH_PROCESAR_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_RMOV_0100_EXT_TRANSF_001_EXTRACT.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **BD_100_TRANS_INFONA**: Extraer registros rechazados desde Oracle con LEFT JOINs
# MAGIC 2. **DS_200_RECH_PROCESAR**: Escribir resultado en tabla Delta de Databricks
# MAGIC 3. El dataset resultante será consumido por jobs subsecuentes en la secuencia
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Secuencia Completa (JQ_PATRIF_RMOV_0100_TRANSF)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JQ_PATRIF_RMOV_0100_TRANSF.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC ### Job Paralelo (JP_PATRIF_RMOV_0100_EXT_TRANSF)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JQ_PATRIF_RMOV_0100_TRANSF_001.png" style="max-width: 100%; height: auto;"/>
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

# DBTITLE 1,EXTRACCIÓN DESDE ORACLE - Registros Rechazados
# Extraer registros de transferencias INFONAVIT rechazados por procesar
statement_extract = query.get_statement(
    "NB_PATRIF_RMOV_0100_EXT_TRANSF_001_EXTRACT.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_TRANS_INFONA=conf.TL_PRO_TRANS_INFONA,
    TL_MATRIZ_CONVIV=conf.TL_CRN_MATRIZ_CONV,
    TL_IND_CTA_INDV=conf.TL_CRN_IND_CTA_INDV,
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

# COMMAND ----------

# DBTITLE 2,DEBUG - Visualizar Datos Extraídos
if conf.debug:
    display(db.read_delta(f"RESULTADO_RECH_PROCESAR_{params.sr_folio}"))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
