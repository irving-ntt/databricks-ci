import pytest


class DummyQuery:
    def __init__(self):
        self.calls = []

    def get_statement(self, name, **kwargs):
        self.calls.append((name, kwargs))
        return f"STMT({name})"


class DummyReadDeltaResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class DummyDB:
    def __init__(self, contador_row=None, raise_before_sql=False):
        self.writes = []
        self.dml_calls = []
        self.contador_row = contador_row
        self.raise_before_sql = raise_before_sql

    def sql_delta(self, statement):
        return f"SQLDATA-{statement}"

    def write_delta(self, table_name, df, mode):
        self.writes.append((table_name, df, mode))

    def read_delta(self, table):
        # return object with first() method
        return DummyReadDeltaResult(self.contador_row)

    def execute_oci_dml(self, statement, async_mode=False):
        # first call (delete) may raise if configured
        if not async_mode and self.raise_before_sql:
            raise RuntimeError("boom")
        self.dml_calls.append((statement, async_mode))
        return f"EXEC-{async_mode}"

    def write_data(self, df, table, database, mode):
        self.writes.append((table, df, mode))


class DummyLogger:
    def __init__(self):
        self.messages = []
        self.warnings = []

    def info(self, msg):
        self.messages.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)


class DummySpark:
    def __init__(self):
        self.created = []

    class DF:
        def __init__(self, data, schema):
            self.data = data
            self.schema = schema
            self.cols = []

        def withColumn(self, name, expr):
            self.cols.append((name, expr))
            return self

    def createDataFrame(self, data, schema):
        df = DummySpark.DF(data, schema)
        self.created.append(df)
        return df


class DummyParams:
    def __init__(self, folio, proceso="1", subproceso="2", usuario="u"):
        self.sr_folio = folio
        self.sr_proceso = proceso
        self.sr_subproceso = subproceso
        self.sr_usuario = usuario


class DummyConf:
    def __init__(self):
        self.CX_PRO_ESQUEMA = "PRO"
        self.TL_PRO_RESPUESTA_ITGY = "RESP"  # output table


# replicate most notebook logic

def run_notebook_logic(params, conf, query, db, logger, spark):
    statement_ag_max = query.get_statement(
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_001_AG_MAX.sql",
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
        SR_FOLIO=params.sr_folio,
    )

    db.write_delta(
        f"TEMP_AG_MAX_{params.sr_folio}", db.sql_delta(statement_ag_max), "overwrite"
    )
    logger.info("✅ AG_804_MAX completado")

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

    statement_sumario = query.get_statement(
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_003_SUMARIO.sql",
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
        SR_FOLIO=params.sr_folio,
    )

    db.write_delta(
        f"TEMP_SUMARIO_{params.sr_folio}", db.sql_delta(statement_sumario), "overwrite"
    )
    logger.info("✅ RD_802_REG_UNIC completado")

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

    # TF_200_REGLAS
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
        "FTD_FEH_CRE", "tzexpr"
    )

    logger.info("✅ TF_200_REGLAS completado")

    # BeforeSQL
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

    tabla_respuesta = f"{conf.CX_PRO_ESQUEMA}.{conf.TL_PRO_RESPUESTA_ITGY}"
    db.write_data(df_respuesta, tabla_respuesta, "default", "append")
    logger.info("✅ DB_500_RESPUESTA_ITGY completado")

    # final summary logs
    logger.info(f"✅ JOB COMPLETADO: {params.sr_folio}")
    logger.info(f"   Registros enviados: {contador_enviados}")


# --- tests -----------------------------------------------------------

def test_normal_flow():
    params = DummyParams(folio="FOL")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB(contador_row={"CONTADOR": 5})
    logger = DummyLogger()
    spark = DummySpark()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "C", "SCHEMA": "S"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger, spark)

    # verify statement sequence
    names = [c[0] for c in query.calls]
    assert names == [
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_001_AG_MAX.sql",
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_002_ENCABEZADO.sql",
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_003_SUMARIO.sql",
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_005_UNION_ORDEN.sql",
        "NB_PATRIF_REVMOV_0300_ARCH_INGTY_006_RESPUESTA_DELETE.sql",
    ]

    # check write operations count and tables
    tables = [w[0] for w in db.writes]
    assert "TEMP_AG_MAX_FOL" in tables
    assert "TEMP_ENCABEZADO_FOL" in tables
    assert "TEMP_SUMARIO_FOL" in tables
    assert "PPA_RCAT069_FOL_3" in tables
    assert "PRO.RESP" in tables  # response insert

    # verify DML calls
    assert db.dml_calls[0][1] is False

    # check spark dataframe created
    assert len(spark.created) == 1
    assert spark.created[0].data[0][3] == 5

    # logger messages include completion and contador
    assert any("JOB COMPLETADO" in m for m in logger.messages)
    assert any("Registros enviados" in m for m in logger.messages)


def test_missing_contador_defaults_zero():
    params = DummyParams(folio="X")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB(contador_row=None)
    logger = DummyLogger()
    spark = DummySpark()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "A", "SCHEMA": "B"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger, spark)

    # contador should be reported zero
    assert any("Registros enviados: 0" in m for m in logger.messages)


def test_before_sql_exception_logged():
    params = DummyParams(folio="Y")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB(contador_row={"CONTADOR": 1}, raise_before_sql=True)
    logger = DummyLogger()
    spark = DummySpark()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "Z", "SCHEMA": "W"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger, spark)

    assert logger.warnings[0].startswith("⚠️ BeforeSQL")


def test_empty_folio_tables_naming():
    params = DummyParams(folio="")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB(contador_row={"CONTADOR": 2})
    logger = DummyLogger()
    spark = DummySpark()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "C", "SCHEMA": "S"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger, spark)

    assert any(t.startswith("TEMP_AG_MAX_") for t in [w[0] for w in db.writes])
    assert any(t.startswith("PRO.") for t in [w[0] for w in db.writes])
