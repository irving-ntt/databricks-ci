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
        self.read_calls = []

    def read_data(self, database, statement):
        self.read_calls.append((database, statement))
        return f"DATA-{database}-{statement}"

    def sql_delta(self, statement):
        return f"SQLDATA-{statement}"

    def write_delta(self, table_name, df, mode):
        self.writes.append((table_name, df, mode))


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


class DummyParams:
    def __init__(self, sr_folio, sr_subproceso=None):
        self.sr_folio = sr_folio
        self.sr_subproceso = sr_subproceso or "SUB"


class DummyConf:
    def __init__(self):
        self.CX_CRN_ESQUEMA = "CRN"
        self.TL_CRN_MAP_NCI_ITGY = "MAPTABLE"


# replicate the notebook's behavior

def run_notebook_logic(params, conf, query, db, logger):
    # first stage: map extraction
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

    # second stage: variable calculation
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


# --- tests -----------------------------------------------------------

def test_full_flow_calls_and_writes():
    params = DummyParams(sr_folio="F7", sr_subproceso="XSUB")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    # set SETTINGS for the variables query
    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "CAT", "SCHEMA": "SCH"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # two statements should have been requested
    assert len(query.calls) == 2
    name1, kw1 = query.calls[0]
    assert name1 == "NB_PATRIF_REVMOV_0200_DESM_NCI_001_MAP_NCI.sql"
    assert kw1["CX_CRN_ESQUEMA"] == "CRN"
    assert kw1["MAP_NCI_ITGY"] == "MAPTABLE"
    assert kw1["SR_SUBPROCESO"] == "XSUB"

    name2, kw2 = query.calls[1]
    assert name2 == "NB_PATRIF_REVMOV_0200_DESM_NCI_002_VARIABLES.sql"
    assert kw2["CATALOG_SCHEMA"] == "CAT.SCH"
    assert kw2["SR_FOLIO"] == "F7"

    # check writes
    assert len(db.writes) == 2
    assert db.writes[0][0] == "TEMP_MAP_NCI_F7"
    assert db.writes[0][2] == "overwrite"
    assert db.writes[1][0] == "TEMP_TR500_CAMPOS_F7"
    assert db.writes[1][2] == "overwrite"

    # check logger messages order
    assert logger.messages[0] == "Mapeo NCI-Integrity extraído"
    assert logger.messages[1] == "Variables del transformer calculadas y guardadas"


def test_empty_folio_and_default_subproceso():
    params = DummyParams(sr_folio="")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "X", "SCHEMA": "Y"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # ensure table names still form correctly
    assert db.writes[0][0] == "TEMP_MAP_NCI_"
    assert db.writes[1][0] == "TEMP_TR500_CAMPOS_"
    # folio parameter for variables is empty string
    assert query.calls[1][1]["SR_FOLIO"] == ""


def test_read_data_used_for_map_stage():
    params = DummyParams(sr_folio="A1")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "C", "SCHEMA": "S"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    # read_data should have been called once with 'default'
    assert len(db.read_calls) == 1
    assert db.read_calls[0][0] == "default"
    assert db.read_calls[0][1].startswith("STMT(")


def test_logging_only_messages():
    params = DummyParams(sr_folio="Z9")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    class FakeSettings:
        GENERAL = type("G", (), {"CATALOG": "CAT", "SCHEMA": "SCH"})()
    globals()["SETTINGS"] = FakeSettings()

    run_notebook_logic(params, conf, query, db, logger)

    assert "Mapeo NCI-Integrity extraído" in logger.messages
    assert "Variables del transformer calculadas y guardadas" in logger.messages
