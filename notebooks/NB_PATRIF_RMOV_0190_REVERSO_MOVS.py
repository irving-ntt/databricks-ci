# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_RMOV_0190_REVERSO_MOVS
# MAGIC
# MAGIC **Descripción:** Notebook decisor que determina qué workflow hijo ejecutar para Reverso de Movimientos
# MAGIC
# MAGIC **Tipo:** Notebook Decisor (Workflow Padre)
# MAGIC
# MAGIC **Función:**
# MAGIC - Evaluar el parámetro `sr_subproceso`
# MAGIC - Determinar workflow hijo a ejecutar según el subproceso
# MAGIC - Retornar variable `job_target` con nombre del workflow
# MAGIC
# MAGIC **Workflows Hijos Posibles:**
# MAGIC - JQ_PATRIF_RMOV_0100_TRNS_FOV (subproceso 363 - Transferencias FOVISSSTE)
# MAGIC - JQ_PATRIF_RMOV_0100_TRANSF (subprocesos 364, 365, 368 - Transferencias Infonavit)
# MAGIC
# MAGIC **Parámetros de Entrada:**
# MAGIC - sr_proceso: ID del proceso
# MAGIC - sr_subproceso: ID del subproceso (363, 364, 365, 368)
# MAGIC - sr_folio: Folio de la operación
# MAGIC - sr_subetapa: Subetapa del proceso
# MAGIC
# MAGIC **Salida:**
# MAGIC - job_target: Nombre del workflow hijo a ejecutar
# MAGIC
# MAGIC **Fecha de Creación:** 2026-02-25
# MAGIC **Autor:** Sistema de Migración Automatizada

# COMMAND ----------
# MAGIC %run "./startup"

# COMMAND ----------
# MAGIC %run "../Notebooks/global_parameters"

# COMMAND ----------
# DBTITLE 1,DEFINICIÓN DE PARÁMETROS

# Definir parámetros del notebook decisor
params = WidgetParams({
    # Parámetros obligatorios del framework
    "var_tramite": str,
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_id_snapshot": str,
    # Parámetros específicos para decisión
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_folio": str,
    "sr_subetapa": str,
    "sr_actualiza": str,
    "sr_fec_acc": str,
    "sr_fec_liq": str,
    "sr_paso": str,
})
params.validate()

# Cargar configuración
conf = ConfManager()

# Logging inicial
logger.info(f"=== Iniciando Notebook Decisor NB_PATRIF_RMOV_0190_REVERSO_MOVS ===")
logger.info(f"Proceso: {params.sr_proceso}")
logger.info(f"Subproceso: {params.sr_subproceso}")
logger.info(f"Folio: {params.sr_folio}")

# COMMAND ----------
# DBTITLE 1,VALIDACIÓN DE PARÁMETROS

# Validar parámetros críticos
if not params.sr_proceso:
    raise ValueError("Parámetro sr_proceso es obligatorio")

if not params.sr_subproceso:
    raise ValueError("Parámetro sr_subproceso es obligatorio")

if not params.sr_folio:
    raise ValueError("Parámetro sr_folio es obligatorio")

logger.info("Validación de parámetros exitosa")

# COMMAND ----------
# DBTITLE 1,LÓGICA DE DECISIÓN

# Workflows válidos
workflows_validos = {
    "TRNS_FOV": "JQ_PATRIF_RMOV_0100_TRNS_FOV",      # Subproceso 363
    "TRANSF": "JQ_PATRIF_RMOV_0100_TRANSF",          # Subprocesos 364, 365, 368
}

try:
    # Determinar workflow según el subproceso
    if params.sr_subproceso == "363":
        job_target = workflows_validos["TRNS_FOV"]
        logger.info(f"Subproceso 363 identificado: Transferencias FOVISSSTE")
    
    elif params.sr_subproceso in ["364", "365", "368"]:
        job_target = workflows_validos["TRANSF"]
        logger.info(f"Subproceso {params.sr_subproceso} identificado: Transferencias Infonavit")
    
    else:
        # Default: usar TRANSF para subprocesos no reconocidos
        job_target = workflows_validos["TRANSF"]
        logger.warning(f"Subproceso {params.sr_subproceso} no reconocido - usando workflow por defecto (TRANSF)")
    
    logger.info(f"Workflow hijo seleccionado: {job_target}")
    
except Exception as e:
    logger.error(f"Error en lógica de decisión: {str(e)}")
    job_target = workflows_validos["TRANSF"]
    logger.info(f"Usando workflow por defecto debido a error: {job_target}")

# COMMAND ----------
# DBTITLE 1,RETORNO DE VALORES

# Preparar valores de retorno
resultado = {
    "job_target": job_target,
    "sr_proceso": params.sr_proceso,
    "sr_subproceso": params.sr_subproceso,
    "sr_folio": params.sr_folio,
    "sr_subetapa": params.sr_subetapa,
    "sr_usuario": params.sr_usuario,
    "sr_etapa": params.sr_etapa,
    "sr_instancia_proceso": params.sr_instancia_proceso,
    "sr_id_snapshot": params.sr_id_snapshot,
    "sr_actualiza": params.sr_actualiza,
    "sr_fec_acc": params.sr_fec_acc,
    "sr_fec_liq": params.sr_fec_liq,
    "sr_paso": params.sr_paso,
    "sr_reproceso": "",  # Parámetro adicional
    "sr_id_archivo": "",  # Parámetro adicional
}

logger.info(f"=== Finalizando Notebook Decisor ===")
logger.info(f"Job Target retornado: {job_target}")

# Retornar valores
dbutils.notebook.exit(json.dumps(resultado))
