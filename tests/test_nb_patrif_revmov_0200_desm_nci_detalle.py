import pytest


class DummyQuery:
    def __init__(self):
        self.last_called = None

    def get_statement(self, name, **kwargs):
        self.last_called = (name, kwargs)
        return f"STATEMENT({name})"


class DummyDB:
    def __init__(self):
        self.written = []

    def sql_delta(self, statement):
        # simulate a dataframe or result from a SQL query
        return f"SQLDATA-{statement}"

    def write_delta(self, table_name, df, mode):
        self.written.append((table_name, df, mode))


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


class DummyParams:
    def __init__(self, sr_folio):
        self.sr_folio = sr_folio


class DummySettings:
    def __init__(self, catalog, schema):
        self.GENERAL = type("G", (), {"CATALOG": catalog, "SCHEMA": schema})()


# helper replicating notebook core logic

def run_notebook_logic(params, settings, query, db, logger):
    statement_detalle = query.get_statement(
        "NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql",
        CATALOG_SCHEMA=f"{settings.GENERAL.CATALOG}.{settings.GENERAL.SCHEMA}",
        SR_FOLIO=params.sr_folio,
    )

    db.write_delta(
        f"RESULTADO_DETALLE_INTEGRITY_{params.sr_folio}",
        db.sql_delta(statement_detalle),
        "overwrite",
    )

    logger.info("Archivo detalle Integrity generado exitosamente")


# --- tests -----------------------------------------------------------

def test_statement_parameters_are_constructed_correctly():
    params = DummyParams(sr_folio="F999")
    settings = DummySettings(catalog="CAT", schema="SCH")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, settings, query, db, logger)

    assert query.last_called is not None
    name, kwargs = query.last_called
    assert name == "NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql"
    assert kwargs["CATALOG_SCHEMA"] == "CAT.SCH"
    assert kwargs["SR_FOLIO"] == "F999"


def test_write_delta_invoked_with_sql_result():
    params = DummyParams(sr_folio="FOO")
    settings = DummySettings(catalog="C", schema="S")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, settings, query, db, logger)

    assert len(db.written) == 1
    table_name, df, mode = db.written[0]
    assert table_name == "RESULTADO_DETALLE_INTEGRITY_FOO"
    assert df == "SQLDATA-STATEMENT(NB_PATRIF_REVMOV_0200_DESM_NCI_004_DETALLE.sql)"
    assert mode == "overwrite"


def test_logger_receives_message():
    params = DummyParams(sr_folio="1")
    settings = DummySettings(catalog="X", schema="Y")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, settings, query, db, logger)

    assert len(logger.messages) == 1
    assert "Archivo detalle Integrity generado exitosamente" in logger.messages[0]


def test_empty_folio_builds_table_name():
    params = DummyParams(sr_folio="")
    settings = DummySettings(catalog="A", schema="B")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, settings, query, db, logger)

    assert db.written[0][0] == "RESULTADO_DETALLE_INTEGRITY_"
