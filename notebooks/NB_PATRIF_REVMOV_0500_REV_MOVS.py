# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0500_REV_MOVS
# MAGIC
# MAGIC **Descripción:** Job paralelo que cambia el estatus en Pre-Movimientos de los registros rechazados por Procesar según subproceso (TA, UG, AG).
# MAGIC
# MAGIC **Subetapa:** Actualización de estatus en Pre-Movimientos
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_RECH_PROCESAR_{sr_folio} (Delta - del job anterior)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS (Oracle - UPDATE mediante DELETE + INSERT)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PRE_MOV_FULL_{sr_folio} (datos completos de Oracle - todos los campos)
# MAGIC - TEMP_ACT_PRE_MOV_{sr_folio} (campos a actualizar desde RESULTADO_RECH_PROCESAR)
# MAGIC - TEMP_FINAL_PRE_MOV_{sr_folio} (datos completos actualizados para INSERT)
# MAGIC
# MAGIC **Archivos SQL (Approach actual):**
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_001_TRANSFORM.sql
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_002_DELETE.sql
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_003_EXTRACT_FULL.sql
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_004_UPDATE_FULL.sql
# MAGIC
# MAGIC **Archivos SQL Alternativos (Approach MERGE - deshabilitados):**
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_003_MERGE.sql.disabled
# MAGIC - NB_PATRIF_REVMOV_0500_REV_MOVS_004_CLEANUP_AUX.sql.disabled
# MAGIC
# MAGIC **NOTA:** En DataStage original se usa Oracle Connector con WriteMode=UPDATE (mode=1).
# MAGIC En Databricks usamos DELETE + INSERT para simplicidad. El approach alternativo con
# MAGIC MERGE está disponible comentado en el notebook (ver sección comentada más abajo).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **EXTRACT_FULL**: Extraer registros completos de Oracle (todos los campos)
# MAGIC 2. **DS_200_RECH_PROCESAR**: Leer registros rechazados
# MAGIC 3. **TR_300_ACT_PRE_MOV**: Transformar campos a actualizar
# MAGIC 4. **UPDATE_FULL**: JOIN para actualizar campos preservando el resto
# MAGIC 5. **DELETE**: Eliminar registros previos en Oracle
# MAGIC 6. **INSERT**: Insertar registros completos actualizados en Oracle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Job Completo (JP_PATRIF_REVMOV_0500_REV_MOVS)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0500_REV_MOVS.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC ## 📊 STAGE: TR_300_ACT_PRE_MOV (Transformer)
# MAGIC
# MAGIC **Derivaciones:**
# MAGIC - FTC_FOLIO = FTC_FOLIO_BITACORA
# MAGIC - FTN_NUM_CTA_INVDUAL = FTN_NUM_CTA_AFORE
# MAGIC - FTN_ID_MARCA = FTN_ID_MARCA
# MAGIC - FCN_ID_SUBPROCESO = p_SR_SUBPROCESO
# MAGIC - FCD_FEH_ACT = CurrentTimestamp()
# MAGIC - FCC_USU_ACT = usuario
# MAGIC - FTN_PRE_MOV_GENERADO = '3' (constante - rechazado)
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage TR_300_ACT_PRE_MOV
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/TR_300_ACT_PRE_MOV.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# Paso 1: Extraer registros completos de Oracle (todos los campos)
statement_extract_full = query.get_statement(
    "NB_PATRIF_REVMOV_0500_REV_MOVS_003_EXTRACT_FULL.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_PRE_MOVIMIENTOS=conf.TL_CRE_ETL_PRE_MOVIMIENTOS,
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_PRE_MOV_FULL_{params.sr_folio}",
    db.read_data("default", statement_extract_full),
    "overwrite",
)

logger.info("✅ Datos completos extraídos de Oracle")

# COMMAND ----------

# Paso 2: Crear tabla de actualización (campos a modificar)
statement_transform = query.get_statement(
    "NB_PATRIF_REVMOV_0500_REV_MOVS_001_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    SR_USUARIO=params.sr_usuario,
)

db.write_delta(
    f"TEMP_ACT_PRE_MOV_{params.sr_folio}",
    db.sql_delta(statement_transform),
    "overwrite",
)

logger.info("✅ TR_300_ACT_PRE_MOV completado")

# COMMAND ----------

# Paso 3: JOIN para actualizar campos específicos preservando el resto
statement_update_full = query.get_statement(
    "NB_PATRIF_REVMOV_0500_REV_MOVS_004_UPDATE_FULL.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.sql_delta(statement_update_full)
# db.write_delta(
#     f"TEMP_FINAL_PRE_MOV_{params.sr_folio}",
#     db.sql_delta(statement_update_full),
#     "overwrite",
# )

if conf.debug:
    display(db.read_delta(f"TEMP_PRE_MOV_FULL_{params.sr_folio}"))

logger.info("✅ Registros actualizados con todos los campos preservados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: BD_400_ETL_PRE_MOVIMIENTOS (Oracle Connector - UPDATE)
# MAGIC
# MAGIC **Connector:** OracleConnector
# MAGIC **Version:** 11g
# MAGIC **WriteMode:** UPDATE (mode=1)
# MAGIC **Table:** CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS
# MAGIC **Keys:** FTC_FOLIO, FTN_NUM_CTA_INVDUAL, FTN_ID_MARCA, FCN_ID_SUBPROCESO
# MAGIC
# MAGIC **Approach Databricks (actual - DELETE + INSERT con datos completos):**
# MAGIC 1. EXTRACT registros completos de Oracle (todos los campos)
# MAGIC 2. TRANSFORM campos a actualizar desde RESULTADO_RECH_PROCESAR
# MAGIC 3. JOIN para actualizar solo campos específicos preservando el resto
# MAGIC 4. DELETE registros previos (por composite key)
# MAGIC 5. INSERT registros completos actualizados
# MAGIC
# MAGIC **Approach Alternativo (MERGE con tabla AUX - comentado más abajo):**
# MAGIC 1. INSERT en CIERREN_DATAUX.TTSISGRAL_ETL_PRE_MOVIMIENTOS_AUX
# MAGIC 2. MERGE desde AUX hacia tabla final
# MAGIC 3. DELETE tabla AUX (cleanup async)
# MAGIC
# MAGIC **NOTA:** DataStage original usa UPDATE directo (WriteMode=1).

# COMMAND ----------

# =============================================================================
# APPROACH ACTUAL: DELETE + INSERT (con datos completos)
# =============================================================================

# Paso 4: DELETE registros previos
statement_delete = query.get_statement(
    "NB_PATRIF_REVMOV_0500_REV_MOVS_002_DELETE.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_PRE_MOVIMIENTOS=conf.TL_CRE_ETL_PRE_MOVIMIENTOS,
    SR_FOLIO=params.sr_folio,
)

db.execute_oci_dml(statement=statement_delete, async_mode=False)

logger.info("✅ DELETE previo completado")

# COMMAND ----------

# DBTITLE 1,approach DELETE + INSERT
# Paso 5: INSERT registros completos actualizados
tabla_pre_movs = f"{conf.CX_CRE_ESQUEMA}.{conf.TL_CRE_ETL_PRE_MOVIMIENTOS}"
df_final_pre_mov = db.read_delta(f"TEMP_PRE_MOV_FULL_{params.sr_folio}")

db.write_data(df_final_pre_mov, tabla_pre_movs, "default", "append")

logger.info(f"✅ INSERT completado - Registros actualizados con todos los campos")

# COMMAND ----------

# DBTITLE 1,approach MERGE
# =============================================================================
# APPROACH ALTERNATIVO: MERGE CON TABLA AUXILIAR (COMENTADO)
# =============================================================================
# Si en el futuro se requiere usar tabla auxiliar para el UPDATE (approach MERGE),
# descomentar el siguiente código y comentar el approach DELETE + INSERT anterior.
#
# PASOS PARA HABILITAR:
# 1. Renombrar archivos SQL:
#    - NB_PATRIF_REVMOV_0500_REV_MOVS_003_MERGE.sql.disabled → .sql
#    - NB_PATRIF_REVMOV_0500_REV_MOVS_004_CLEANUP_AUX.sql.disabled → .sql
# 2. Crear tabla auxiliar en Oracle (DDL abajo)
# 3. Descomentar código siguiente
# 4. Comentar approach DELETE + INSERT anterior
#
# DDL TABLA AUXILIAR:
# CREATE TABLE CIERREN_DATAUX.TTSISGRAL_ETL_PRE_MOVIMIENTOS_AUX (
#     FTC_FOLIO VARCHAR2(30),
#     FTN_NUM_CTA_INVDUAL NUMBER(10,0),
#     FTN_ID_MARCA NUMBER(10,0),
#     FCN_ID_SUBPROCESO NUMBER(10,0),
#     FCD_FEH_ACT TIMESTAMP,
#     FCC_USU_ACT VARCHAR2(30),
#     FTN_PRE_MOV_GENERADO NUMBER(1,0)
# );
# =============================================================================

# # Paso 1: INSERT en tabla AUX
# from pyspark.sql.functions import lit
#
# df_act_pre_mov = db.read_delta(f"TEMP_ACT_PRE_MOV_{params.sr_folio}")
#
# tabla_aux = f"{conf.CX_DATAUX_ESQUEMA}.TTSISGRAL_ETL_PRE_MOVIMIENTOS_AUX"
# db.write_data(df_act_pre_mov, tabla_aux, "default", "append")
#
# logger.info("✅ Datos insertados en tabla AUX")

# # Paso 2: MERGE desde AUX hacia tabla final
# # Crear archivo SQL: NB_PATRIF_REVMOV_0500_REV_MOVS_003_MERGE.sql
# # MERGE INTO CIERREN_ETL.TTSISGRAL_ETL_PRE_MOVIMIENTOS target
# # USING CIERREN_DATAUX.TTSISGRAL_ETL_PRE_MOVIMIENTOS_AUX source
# # ON (target.FTC_FOLIO = source.FTC_FOLIO
# #     AND target.FTN_NUM_CTA_INVDUAL = source.FTN_NUM_CTA_INVDUAL
# #     AND target.FTN_ID_MARCA = source.FTN_ID_MARCA
# #     AND target.FCN_ID_SUBPROCESO = source.FCN_ID_SUBPROCESO)
# # WHEN MATCHED THEN
# #     UPDATE SET
# #         target.FCD_FEH_ACT = source.FCD_FEH_ACT,
# #         target.FCC_USU_ACT = source.FCC_USU_ACT,
# #         target.FTN_PRE_MOV_GENERADO = source.FTN_PRE_MOV_GENERADO
#
# statement_merge = query.get_statement(
#     "NB_PATRIF_REVMOV_0500_REV_MOVS_003_MERGE.sql",
#     CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
#     TL_PRE_MOVIMIENTOS=conf.TL_CRE_ETL_PRE_MOVIMIENTOS,
#     CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
# )
#
# db.execute_oci_dml(statement=statement_merge, async_mode=False)
#
# logger.info("✅ MERGE completado")

# # Paso 3: CLEANUP - DELETE tabla AUX (async)
# # Crear archivo SQL: NB_PATRIF_REVMOV_0500_REV_MOVS_004_CLEANUP_AUX.sql
# # DELETE FROM CIERREN_DATAUX.TTSISGRAL_ETL_PRE_MOVIMIENTOS_AUX
# # WHERE FTC_FOLIO = '${SR_FOLIO}'
#
# statement_cleanup = query.get_statement(
#     "NB_PATRIF_REVMOV_0500_REV_MOVS_004_CLEANUP_AUX.sql",
#     CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
#     SR_FOLIO=params.sr_folio,
# )
#
# db.execute_oci_dml(statement=statement_cleanup, async_mode=True)
#
# logger.info(f"✅ Tabla AUX cleanup iniciado (async) - Folio: {params.sr_folio}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Job Completado

# COMMAND ----------

logger.info("=" * 80)
logger.info(f"✅ JOB COMPLETADO: {params.sr_folio}")
logger.info(f"   Tabla actualizada: {tabla_pre_movs}")
logger.info(f"   Estatus: FTN_PRE_MOV_GENERADO = 3 (Rechazado)")
logger.info("=" * 80)

Notify.send_notification("WF_PATRIF_RMOV_0100_TRANSF", params)
CleanUpManager.cleanup_notebook(locals())
