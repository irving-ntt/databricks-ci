import pytest


class DummyQuery:
    def __init__(self):
        self.calls = []

    def get_statement(self, name, **kwargs):
        self.calls.append((name, kwargs))
        return f"STMT({name})"


class DummyDB:
    def __init__(self):
        self.writes = []
        self.dml_calls = []

    def sql_delta(self, statement):
        return f"SQLDATA-{statement}"

    def write_data(self, df, table, database, mode):
        self.writes.append((df, table, database, mode))

    def execute_oci_dml(self, statement, async_mode):
        self.dml_calls.append((statement, async_mode))
        return f"EXEC-{async_mode}"


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


class DummyParams:
    def __init__(self, sr_folio):
        self.sr_folio = sr_folio


class DummyConf:
    def __init__(self):
        self.CX_DATAUX_ESQUEMA = "DATAUX"
        self.TL_DATAUX_MATRIZ_CONV_AUX = "AUX_TABLE"
        self.CX_CRN_ESQUEMA = "CRN"
        self.TL_CRN_MATRIZ_CONV = "MATRIZ"
        self.CX_CRE_USUARIO = "USU"


# replicate the notebook's logic in a helper

def run_notebook_logic(params, conf, query, db, logger):
    statement_prepare = query.get_statement(
        "NB_PATRIF_REVMOV_0200_DESM_NCI_003_PREPARE_AUX.sql",
        CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
        SR_FOLIO=f"{params.sr_folio}",
    )

    db.write_data(
        db.sql_delta(statement_prepare),
        f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DATAUX_MATRIZ_CONV_AUX}",
        "default",
        "append",
    )
    logger.info("Datos insertados en tabla auxiliar")

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
    db.execute_oci_dml(statement=statement_merge, async_mode=False)
    logger.info("MERGE ejecutado exitosamente")

    statement_delete = query.get_statement(
        "NB_PATRIF_REVMOV_0200_DESM_NCI_007_DELETE_AUX.sql",
        hints="/*+ PARALLEL(8) */",
        CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
        TL_DATAUX_MATRIZ_CONV_AUX=conf.TL_DATAUX_MATRIZ_CONV_AUX,
        SR_FOLIO=f"'{params.sr_folio}'",
    )
    db.execute_oci_dml(statement=statement_delete, async_mode=True)


# --- tests -----------------------------------------------------------

def test_prepare_and_write():
    params = DummyParams(sr_folio="F1")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    # monkeypatch global SETTINGS used in call
    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "CAT", "SCHEMA": "SCH"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # first call should be prepare statement
    assert query.calls[0][0] == "NB_PATRIF_REVMOV_0200_DESM_NCI_003_PREPARE_AUX.sql"
    assert query.calls[0][1]["SR_FOLIO"] == "F1"

    assert len(db.writes) == 1
    df, table, database, mode = db.writes[0]
    assert table == "DATAUX.AUX_TABLE"
    assert database == "default"
    assert mode == "append"
    assert df.startswith("SQLDATA-")


def test_merge_and_delete_dml_calls():
    params = DummyParams(sr_folio="FOO")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "C", "SCHEMA": "S"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # merge should be second statement
    assert query.calls[1][0] == "NB_PATRIF_REVMOV_0200_DESM_NCI_006_MERGE_MATRIZ.sql"
    assert "hints" in query.calls[1][1]
    # delete should be third statement
    assert query.calls[2][0] == "NB_PATRIF_REVMOV_0200_DESM_NCI_007_DELETE_AUX.sql"

    assert len(db.dml_calls) == 2
    # first call async_mode False, second True
    assert db.dml_calls[0][1] is False
    assert db.dml_calls[1][1] is True


def test_logging_messages():
    params = DummyParams(sr_folio="X")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "C", "SCHEMA": "S"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    assert "Datos insertados en tabla auxiliar" in logger.messages[0]
    assert "MERGE ejecutado exitosamente" in logger.messages[1]


def test_empty_folio_handling():
    params = DummyParams(sr_folio="")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "A", "SCHEMA": "B"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # ensure quoted folio becomes '' in merge/delete parameters
    assert query.calls[1][1]["SR_FOLIO"] == "''"
    assert query.calls[2][1]["SR_FOLIO"] == "''"
