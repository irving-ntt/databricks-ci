# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0300_ARCH_INGTY
# MAGIC
# MAGIC **Descripción:** Job paralelo que crea el archivo de desmarca para Integrity, según corresponda el subproceso (TA, UG, AG). Genera un archivo de texto con formato fijo de 128 caracteres por línea y registra el envío en la tabla de respuesta Oracle.
# MAGIC
# MAGIC **Subetapa:** Generación de archivo para desmarca en Integrity
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_DETALLE_INTEGRITY_{sr_folio} (Delta - del job 0200 DESM_NCI)
# MAGIC - RESULTADO_SUMARIO_INTEGRITY_{sr_folio} (Delta - del job 0200 DESM_NCI)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - PROCESOS.TTAFOGRAL_RESPUESTA_ITGY (Oracle - registro de envío a Integrity)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_AG_MAX_{sr_folio} (resultado de AG_804_MAX: FTC_FOLIO_BITACORA, MAX(CONTADOR))
# MAGIC - TEMP_ENCABEZADO_{sr_folio} (registro tipo '01' - encabezado)
# MAGIC - TEMP_SUMARIO_{sr_folio} (registros tipo '99' - sumario sin duplicados)
# MAGIC - PPA_RCAT069_{sr_folio}_3 (archivo completo ordenado - equivalente SF_809_DESMARCA_INGTY)
# MAGIC
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0300_ARCH_INGTY_001_AG_MAX.sql
# MAGIC - NB_PATRIF_REVMOV_0300_ARCH_INGTY_002_ENCABEZADO.sql
# MAGIC - NB_PATRIF_REVMOV_0300_ARCH_INGTY_003_SUMARIO.sql
# MAGIC - NB_PATRIF_REVMOV_0300_ARCH_INGTY_005_UNION_ORDEN.sql
# MAGIC - NB_PATRIF_REVMOV_0300_ARCH_INGTY_006_RESPUESTA_DELETE.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **CP_803_SEPARA + AG_804_MAX**: Calcular MAX(CONTADOR) por FTC_FOLIO_BITACORA → TEMP_AG_MAX
# MAGIC 2. **TR_805_ENCABEZADO + RD_806_REG_UNIC**: Generar encabezado tipo '01' → TEMP_ENCABEZADO
# MAGIC 3. **RD_802_REG_UNIC**: Eliminar duplicados del sumario → TEMP_SUMARIO
# MAGIC 4. **FN_807_UNE + SR_808_ORDENA**: Unir 3 flujos y ordenar → PPA_RCAT069_{folio}_3
# MAGIC 5. **SF_809_DESMARCA_INGTY**: Tabla Delta con registros ordenados (128 caracteres por línea)
# MAGIC 6. **TF_200_REGLAS + DB_500_RESPUESTA_ITGY**: Insertar registro en Oracle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Job Completo (JP_PATRIF_REVMOV_0300_ARCH_INGTY)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0300_ARCH_INGTY.png" style="max-width: 100%; height: auto;"/>
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
# MAGIC ## 📊 STAGE: DS_800_DETALLE (Data Set Input)
# MAGIC
# MAGIC **Tabla Delta:** `RESULTADO_DETALLE_INTEGRITY_{folio}`
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage DS_800_DETALLE
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/DS_800_DETALLE.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: DS_900_SUMARIO (Data Set Input)
# MAGIC
# MAGIC **Tabla Delta:** `RESULTADO_SUMARIO_INTEGRITY_{folio}`
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage DS_900_SUMARIO
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/DS_900_SUMARIO.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: CP_803_SEPARA (Copy)
# MAGIC
# MAGIC **Input:** DS_800_DETALLE
# MAGIC **Output 1:** FTC_FOLIO_BITACORA, CONTADOR → AG_804_MAX
# MAGIC **Output 2:** ID_REGISTRO, DETALLE → FN_807_UNE
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage CP_803_SEPARA
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/CP_803_SEPARA.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: AG_804_MAX (Aggregator)
# MAGIC
# MAGIC **Operator:** group
# MAGIC **Method:** hash
# MAGIC **Key:** FTC_FOLIO_BITACORA
# MAGIC **Reduce:** MAX(CONTADOR)
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage AG_804_MAX
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/AG_804_MAX.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

statement_ag_max = query.get_statement(
    "NB_PATRIF_REVMOV_0300_ARCH_INGTY_001_AG_MAX.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_AG_MAX_{params.sr_folio}", db.sql_delta(statement_ag_max), "overwrite"
)

logger.info("✅ AG_804_MAX completado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: CP_804_SEPARA (Copy)
# MAGIC
# MAGIC **Input:** AG_804_MAX
# MAGIC **Output 1:** FTC_FOLIO_BITACORA, CONTADOR → TR_805_ENCABEZADO
# MAGIC **Output 2:** CONTADOR → TF_200_REGLAS
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage CP_804_SEPARA
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/CP_804_SEPARA.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: TR_805_ENCABEZADO (Transformer)
# MAGIC
# MAGIC **Variables de Stage:**
# MAGIC - vFECHA = DateToString(CurrentDate(), "%yyyy%mm%dd")
# MAGIC - vNUMREG = Right(TrimB(Change(DecimalToString(CONTADOR), ".", "")), 9)
# MAGIC
# MAGIC **Output:**
# MAGIC - ID_REGISTRO = '01'
# MAGIC - DETALLE = '01' : vFECHA : vNUMREG : Str(' ', 109)
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage TR_805_ENCABEZADO
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/TR_805_ENCABEZADO.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: RD_806_REG_UNIC (Remove Duplicates)
# MAGIC
# MAGIC **Operator:** remdup
# MAGIC **Keep:** first
# MAGIC **Key:** ID_REGISTRO
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage RD_806_REG_UNIC
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/RD_806_REG_UNIC.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

statement_encabezado = query.get_statement(
    "NB_PATRIF_REVMOV_0300_ARCH_INGTY_002_ENCABEZADO.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_ENCABEZADO_{params.sr_folio}",
    db.sql_delta(statement_encabezado),
    "overwrite",
)

logger.info("✅ TR_805_ENCABEZADO + RD_806_REG_UNIC completados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: RD_802_REG_UNIC (Remove Duplicates)
# MAGIC
# MAGIC **Operator:** remdup
# MAGIC **Keep:** first
# MAGIC **Key:** ID_REGISTRO
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage RD_802_REG_UNIC
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/RD_802_REG_UNIC.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

statement_sumario = query.get_statement(
    "NB_PATRIF_REVMOV_0300_ARCH_INGTY_003_SUMARIO.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_SUMARIO_{params.sr_folio}", db.sql_delta(statement_sumario), "overwrite"
)

logger.info("✅ RD_802_REG_UNIC completado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: FN_807_UNE (Funnel)
# MAGIC
# MAGIC **Operator:** funnel
# MAGIC **Inputs:** 3 (sumario, detalle, encabezado)
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage FN_807_UNE
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/FN_807_UNE.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: SR_808_ORDENA (Sort)
# MAGIC
# MAGIC **Operator:** tsort (total sort)
# MAGIC **Stable:** stable (mantiene orden relativo)
# MAGIC **Unique:** No (permite duplicados)
# MAGIC **Key:** ID_REGISTRO
# MAGIC **Direction:** ASC (ascendente)
# MAGIC **Execmode:** seq (sequential - 1 partición)
# MAGIC **Output:** Solo campo DETALLE
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage SR_808_ORDENA
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/SR_808_ORDENA.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

statement_union = query.get_statement(
    "NB_PATRIF_REVMOV_0300_ARCH_INGTY_005_UNION_ORDEN.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"PPA_RCAT069_{params.sr_folio}_3",
    db.sql_delta(statement_union),
    "overwrite",
)

logger.info("✅ FN_807_UNE + SR_808_ORDENA completados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: SF_809_DESMARCA_INGTY (Sequential File)
# MAGIC
# MAGIC **Operator:** export
# MAGIC **Output:** Tabla Delta con registros ordenados
# MAGIC **Formato:** DETALLE (128 caracteres por línea)
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage SF_809_DESMARCA_INGTY
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/SF_809_DESMARCA_INGTY.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

logger.info(
    f"✅ SF_809_DESMARCA_INGTY: Tabla Delta creada - PPA_RCAT069_{params.sr_folio}_3"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: TF_200_REGLAS (Transformer)
# MAGIC
# MAGIC **Input:** CONTADOR (del AG_804_MAX)
# MAGIC **Output:** Registro para Oracle
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage TF_200_REGLAS
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/TF_200_REGLAS.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: DB_500_RESPUESTA_ITGY (Oracle Connector)
# MAGIC
# MAGIC **Connector:** OracleConnector
# MAGIC **Version:** 11g
# MAGIC **Table:** PROCESOS.TTAFOGRAL_RESPUESTA_ITGY
# MAGIC **BeforeSQL:** DELETE por FTC_FOLIO
# MAGIC
# MAGIC ### 🖼️ IMAGEN: Stage DB_500_RESPUESTA_ITGY
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/DB_500_RESPUESTA_ITGY.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# TF_200_REGLAS: Obtener CONTADOR de AG_804_MAX
from pyspark.sql.functions import current_timestamp, from_utc_timestamp

# Obtener contador de registros enviados (manejo de caso vacío)
row_contador = db.read_delta(f"TEMP_AG_MAX_{params.sr_folio}").first()
contador_enviados = row_contador["CONTADOR"] if row_contador else 0

df_respuesta = spark.createDataFrame(
    [
        (
            params.sr_folio,
            int(params.sr_proceso),
            int(params.sr_subproceso),
            int(contador_enviados),
            0,
            3879,
            params.sr_usuario,
            None,
        )
    ],
    schema="""
        FTC_FOLIO STRING,
        FTN_ID_PROCESO INT,
        FTN_ID_SUBPROCESO INT,
        FTN_REGISTROS_ENVIADOS INT,
        FTN_REGISTROS_RECIBIDOS INT,
        FTN_ID_ESTATUS INT,
        FTC_USU_CRE STRING,
        FTD_FEH_CRE TIMESTAMP
    """,
).withColumn(
    "FTD_FEH_CRE", from_utc_timestamp(current_timestamp(), "America/Mexico_City")
)

logger.info("✅ TF_200_REGLAS completado")

# COMMAND ----------

# DB_500_RESPUESTA_ITGY: BeforeSQL
statement_delete = query.get_statement(
    "NB_PATRIF_REVMOV_0300_ARCH_INGTY_006_RESPUESTA_DELETE.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_RESPUESTA_ITGY=conf.TL_PRO_RESPUESTA_ITGY,
    SR_FOLIO=params.sr_folio,
)

try:
    db.execute_oci_dml(statement=statement_delete, async_mode=False)
    logger.info("✅ BeforeSQL ejecutado")
except Exception as e:
    logger.warning(f"⚠️ BeforeSQL: {str(e)}")

# COMMAND ----------

# DB_500_RESPUESTA_ITGY: INSERT
tabla_respuesta = f"{conf.CX_PRO_ESQUEMA}.{conf.TL_PRO_RESPUESTA_ITGY}"

db.write_data(df_respuesta, tabla_respuesta, "default", "append")

logger.info("✅ DB_500_RESPUESTA_ITGY completado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Job Completado

# COMMAND ----------

logger.info("=" * 80)
logger.info(f"✅ JOB COMPLETADO: {params.sr_folio}")
logger.info(f"   Tabla Delta: PPA_RCAT069_{params.sr_folio}_3")
logger.info(f"   Registros enviados: {contador_enviados}")
logger.info("=" * 80)

CleanUpManager.cleanup_notebook(locals())
