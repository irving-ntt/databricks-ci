# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0400_DESACT_INDIC
# MAGIC
# MAGIC **Descripción:** Job paralelo que desactiva el indicador de crédito de vivienda de los registros rechazados por Procesar según subproceso (TA, UG, AG).
# MAGIC
# MAGIC **Subetapa:** Desactivación de indicadores en matriz convivencia
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_RECH_PROCESAR_{sr_folio} (Delta - del job anterior)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - CIERREN.TTAFOGRAL_IND_CTA_INDV (Oracle - UPDATE mediante MERGE)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DESACT_INDIC_{sr_folio} (registros transformados para UPDATE)
# MAGIC
# MAGIC **Tablas Auxiliares (Oracle):**
# MAGIC - CIERREN_DATAUX.TTAFOGRAL_IND_CTA_INDV_AUX (tabla auxiliar para MERGE)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0400_DESACT_INDIC_001_TRANSFORM.sql
# MAGIC - NB_PATRIF_REVMOV_0400_DESACT_INDIC_002_MERGE.sql
# MAGIC - NB_PATRIF_REVMOV_0400_DESACT_INDIC_003_CLEANUP_AUX.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_200_RECH_PROCESAR**: Leer registros rechazados
# MAGIC 2. **TR_300_DESACT_INDI**: Transformar registros para desactivación
# MAGIC 3. **INSERT AUX**: Insertar en tabla auxiliar Oracle
# MAGIC 4. **MERGE**: Actualizar tabla final desde auxiliar
# MAGIC 5. **CLEANUP AUX**: Limpiar tabla auxiliar (async)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Job Completo (JP_PATRIF_REVMOV_0400_DESACT_INDIC)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0400_DESACT_INDIC.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

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

conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: DS_200_RECH_PROCESAR (Data Set Input)
# MAGIC
# MAGIC **Tabla Delta:** `RESULTADO_RECH_PROCESAR_{folio}`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: TR_300_DESACT_INDI (Transformer)
# MAGIC
# MAGIC **Derivaciones:**
# MAGIC - FCN_ID_IND_CTA_INDV = (del input)
# MAGIC - FTN_NUM_CTA_INVDUAL = FTN_NUM_CTA_AFORE
# MAGIC - FFN_ID_CONFIG_INDI = 14 (constante)
# MAGIC - FCC_VALOR_IND = IF subproceso=368 THEN '1' ELSE IF subproceso=364 THEN '2' ELSE '3'
# MAGIC - FTC_VIGENCIA = '0' (desactivar)
# MAGIC - FTD_FEH_ACT = CurrentTimestamp()
# MAGIC - FCC_USU_ACT = usuario
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage TR_300_DESACT_INDI
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/TR_300_DESACT_INDI.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

statement_transform = query.get_statement(
    "NB_PATRIF_REVMOV_0400_DESACT_INDIC_001_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_USUARIO=params.sr_usuario,
)

db.write_delta(
    f"TEMP_DESACT_INDIC_{params.sr_folio}",
    db.sql_delta(statement_transform),
    "overwrite",
)

logger.info("✅ TR_300_DESACT_INDI completado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: BD_400_IND_CTA_INDV (Oracle Connector - UPDATE)
# MAGIC
# MAGIC **Connector:** OracleConnector
# MAGIC **Version:** 11g
# MAGIC **WriteMode:** UPDATE (mode=1)
# MAGIC **Table:** CIERREN.TTAFOGRAL_IND_CTA_INDV
# MAGIC **Keys:** FCN_ID_IND_CTA_INDV, FTN_NUM_CTA_INVDUAL, FFN_ID_CONFIG_INDI
# MAGIC
# MAGIC **Approach Databricks:**
# MAGIC 1. INSERT en tabla AUX
# MAGIC 2. MERGE desde AUX hacia tabla final
# MAGIC 3. DELETE tabla AUX (cleanup async)

# COMMAND ----------

# Paso 1: INSERT en tabla AUX
df_desact = db.read_delta(f"TEMP_DESACT_INDIC_{params.sr_folio}")

# Agregar FTC_FOLIO para poder hacer DELETE después
from pyspark.sql.functions import lit

df_aux = df_desact.withColumn("FTC_FOLIO", lit(params.sr_folio))

tabla_aux = f"{conf.CX_DATAUX_ESQUEMA}.TTAFOGRAL_IND_CTA_INDV_AUX"

db.write_data(df_aux, tabla_aux, "default", "append")

logger.info(f"✅ Datos insertados en tabla AUX")

# COMMAND ----------

# Paso 2: MERGE desde AUX hacia tabla final
statement_merge = query.get_statement(
    "NB_PATRIF_REVMOV_0400_DESACT_INDIC_002_MERGE.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_IND_CTA_INDV=conf.TL_CRN_IND_CTA_INDV,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
)

db.execute_oci_dml(statement=statement_merge, async_mode=False)

logger.info("✅ MERGE completado - Indicadores desactivados")

# COMMAND ----------

# Paso 3: CLEANUP - DELETE tabla AUX (async)
statement_cleanup = query.get_statement(
    "NB_PATRIF_REVMOV_0400_DESACT_INDIC_003_CLEANUP_AUX.sql",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    SR_FOLIO=params.sr_folio,
)

db.execute_oci_dml(statement=statement_cleanup, async_mode=True)

logger.info("✅ Tabla AUX cleanup iniciado (async)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Job Completado

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
