# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REVMOV_0200_DESM_NCI (Principal)
# MAGIC
# MAGIC **Descripción:** Job paralelo que realiza la desmarca de las cuentas en NCI y genera archivos para realizar la desmarca en Integrity, según corresponda el subproceso (TA, UG, AG).
# MAGIC
# MAGIC **Subetapa:** Desmarca NCI y generación de archivos Integrity
# MAGIC
# MAGIC **Trámite:** Reverso de Movimientos - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - RESULTADO_RECH_PROCESAR_{sr_folio} (Delta - del job anterior)
# MAGIC - CIERREN.TMSISGRAL_MAP_NCI_ITGY (Oracle - mapeo NCI-Integrity)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MAP_NCI_{sr_folio} (mapeo de claves)
# MAGIC - TEMP_TR500_CAMPOS_{sr_folio} (con variables calculadas - para uso de notebooks separados)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_001_MAP_NCI.sql
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_002_VARIABLES.sql
# MAGIC
# MAGIC **Notebooks Subsecuentes:**
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_NCI.py (Salida NCI - UPDATE Oracle)
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_DETALLE.py (Salida DETALLE - Dataset sufijo 02)
# MAGIC - NB_PATRIF_REVMOV_0200_DESM_NCI_SUMARIO.py (Salida SUMARIO - Dataset sufijo 03)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job Principal:
# MAGIC 1. **DB_300_MAP_NCI_ITGY**: Extraer mapeo de claves NCI-Integrity desde Oracle
# MAGIC 2. **LK_400_CVES + TR_500_CAMPOS**: JOIN entre registros rechazados (sufijo 01) y mapeo, calcular variables
# MAGIC 3. Guardar resultado con variables calculadas en tabla Delta intermedia
# MAGIC 4. Los notebooks separados procesarán las 3 salidas del transformer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Diagramas DataStage Originales:
# MAGIC
# MAGIC ### Secuencia Completa (JQ_PATRIF_RMOV_0100_TRANSF)
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
# MAGIC ## 📊 STAGE: DB_300_MAP_NCI_ITGY
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_001.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Extraer Mapeo NCI-Integrity
statement_map_nci = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_001_MAP_NCI.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    MAP_NCI_ITGY=conf.TL_CRN_MAP_NCI_ITGY,
    DES_PROCESO="'PROCESOS'",
    SR_SUBPROCESO=params.sr_subproceso,
)

db.write_delta(
    f"TEMP_MAP_NCI_{params.sr_folio}",
    db.read_data("default", statement_map_nci),
    "overwrite",
)

logger.info("Mapeo NCI-Integrity extraído")

# COMMAND ----------

# DBTITLE 2,DEBUG
if conf.debug:
    display(db.read_delta(f"RESULTADO_RECH_PROCESAR_{params.sr_folio}"))
    display(db.read_delta(f"TEMP_MAP_NCI_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 STAGE: LK_400_CVES (Lookup) + TR_500_CAMPOS (Transformer)
# MAGIC
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_002.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC <li>
# MAGIC   <a href="" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/19_REVERSO_MOVS/02_TRANSFERENCIAS/INFONAVIT/Notebooks/assets/JP_PATRIF_REVMOV_0200_DESM_NCI_003.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>
# MAGIC
# MAGIC **Propósito:** Realizar lookup de claves Integrity y calcular todas las variables de stage del transformer.
# MAGIC
# MAGIC **Lookup LK_400_CVES:**
# MAGIC - Keys: PROCESO, SUBPROCESO
# MAGIC - Comportamiento: ifNotFound = FAIL (INNER JOIN)
# MAGIC - Agrega campos: TIPO_PROCESO, TIPO_SUBPROCESO
# MAGIC
# MAGIC **Variables Calculadas (TR_500_CAMPOS):**
# MAGIC - **vFechaEnvio**: Fecha actual en formato YYYYMMDD (8 caracteres)
# MAGIC - **vNSS**: NSS del trabajador (11 caracteres, espacios si NULL)
# MAGIC - **vCURP**: CURP del trabajador (18 caracteres, espacios si NULL)
# MAGIC - **vPROCESO**: Código Integrity del proceso (4 caracteres)
# MAGIC - **vSUBPROCESO**: Código Integrity del subproceso (4 caracteres)
# MAGIC - **vCTA**: Número de cuenta (10 caracteres)
# MAGIC - **CONTADOR**: Número de fila secuencial
# MAGIC
# MAGIC **Uso:** Estas variables serán utilizadas por los 3 notebooks de salida para generar:
# MAGIC 1. UPDATE en matriz convivencia (NCI)
# MAGIC 2. Archivo de detalle para Integrity (DETALLE)
# MAGIC 3. Archivo de sumario para Integrity (SUMARIO)

# COMMAND ----------

# DBTITLE 2,Lookup y Cálculo de Variables
statement_variables = query.get_statement(
    "NB_PATRIF_REVMOV_0200_DESM_NCI_002_VARIABLES.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

db.write_delta(
    f"TEMP_TR500_CAMPOS_{params.sr_folio}",
    db.sql_delta(statement_variables),
    "overwrite",
)

logger.info("Variables del transformer calculadas y guardadas")

# COMMAND ----------

# DBTITLE 3,DEBUG
if conf.debug:
    display(db.read_delta(f"TEMP_TR500_CAMPOS_{params.sr_folio}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ NOTEBOOK PRINCIPAL COMPLETADO
# MAGIC
# MAGIC La tabla Delta `TEMP_TR500_CAMPOS_{sr_folio}` contiene:
# MAGIC - Todos los campos originales del dataset sufijo 01
# MAGIC - Campos del lookup de mapeo NCI-Integrity
# MAGIC - Todas las variables calculadas del transformer
# MAGIC
# MAGIC **Siguiente paso:** Ejecutar los 3 notebooks de salida:
# MAGIC 1. **NB_PATRIF_REVMOV_0200_DESM_NCI_NCI** - Actualizar matriz convivencia
# MAGIC 2. **NB_PATRIF_REVMOV_0200_DESM_NCI_DETALLE** - Generar dataset detalle (sufijo 02)
# MAGIC 3. **NB_PATRIF_REVMOV_0200_DESM_NCI_SUMARIO** - Generar dataset sumario (sufijo 03)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
