# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0200_DESM_NCI_NCI (Salida NCI)
# MAGIC
# MAGIC **Descripción:** Notebook de salida NCI que actualiza la tabla MATRIZ_CONVIVENCIA en Oracle para desmarcar cuentas.
# MAGIC
# MAGIC **Subetapa:** Desmarca de cuentas en NCI
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - TEMP_TR500_CAMPOS_{sr_folio} (Delta - con variables calculadas)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA (Oracle - UPDATE)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_003_PREPARE_AUX.sql
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_006_MERGE_MATRIZ.sql
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_007_DELETE_AUX.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Notebook:
# MAGIC 1. Leer tabla Delta con variables calculadas
# MAGIC 2. Insertar registros en tabla auxiliar (CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX)
# MAGIC 3. Ejecutar MERGE hacia tabla final (MATRIZ_CONVIVENCIA)
# MAGIC 4. Limpiar tabla auxiliar (DELETE)
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
# MAGIC ## 🔄 SALIDA NCI: ACTUALIZAR MATRIZ CONVIVENCIA
# MAGIC
# MAGIC **Patrón:** INSERT → MERGE → DELETE
# MAGIC
# MAGIC **Tablas:**
# MAGIC - Auxiliar: CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX
# MAGIC - Final: CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
# MAGIC
# MAGIC **Campos:**
# MAGIC - `FTB_ESTATUS_MARCA = '0'` (desmarcado)
# MAGIC - `FTD_FEH_ACT = CURRENT_TIMESTAMP`
# MAGIC - `FCC_USU_ACT = usuario`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: BD_600_MATRIZ_CONVIV (UPDATE Oracle)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_006.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,INSERT en Tabla Auxiliar
statement_prepare = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_003_PREPARE_AUX.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=f"{params.sr_folio}",
    #CX_CRE_USUARIO=f"'{conf.CX_CRE_USUARIO}'",
)

db.write_data(
    db.sql_delta(statement_prepare),
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DATAUX_MATRIZ_CONV_AUX}",
    "default",
    "append",
)

logger.info("Datos insertados en tabla auxiliar")

# COMMAND ----------

# DBTITLE 2,MERGE a Tabla Final
statement_merge = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_006_MERGE_MATRIZ.sql",
    hints="/*+ PARALLEL(8) */",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_MATRIZ_CONVIV=conf.TL_CRN_MATRIZ_CONV,
    TL_DATAUX_MATRIZ_CONV_AUX=conf.TL_DATAUX_MATRIZ_CONV_AUX,
    SR_FOLIO=f"'{params.sr_folio}'",
    CX_CRE_USUARIO=f"'{conf.CX_CRE_USUARIO}'",
)

execution = db.execute_oci_dml(statement=statement_merge, async_mode=False)

logger.info("MERGE ejecutado exitosamente")

# COMMAND ----------

# DBTITLE 3,Limpiar Tabla Auxiliar
statement_delete = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_007_DELETE_AUX.sql",
    hints="/*+ PARALLEL(8) */",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DATAUX_MATRIZ_CONV_AUX=conf.TL_DATAUX_MATRIZ_CONV_AUX,
    SR_FOLIO=f"'{params.sr_folio}'",
)

execution = db.execute_oci_dml(statement=statement_delete, async_mode=True)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
