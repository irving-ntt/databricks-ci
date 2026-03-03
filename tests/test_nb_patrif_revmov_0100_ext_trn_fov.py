import pytest


class DummyQuery:
    def __init__(self):
        self.last_called = None

    def get_statement(self, name, **kwargs):
        # record the parameters for inspection and return a fake SQL string
        self.last_called = (name, kwargs)
        return f"STATEMENT({name})"


class DummyDB:
    def __init__(self):
        self.written = []

    def read_data(self, database, statement):
        # simulate a Spark DataFrame by returning a simple string
        return f"DATA-{database}-{statement}"

    def write_delta(self, table_name, df, mode):
        self.written.append((table_name, df, mode))


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


class DummyParams:
    def __init__(self, sr_folio, sr_subproceso=None, sr_proceso=None):
        self.sr_folio = sr_folio
        # some notebooks may pass these; default them to simple values
        self.sr_subproceso = sr_subproceso or "X"
        self.sr_proceso = sr_proceso or "Y"


class DummyConf:
    def __init__(self):
        # minimal attributes needed by the notebook
        self.CX_PRO_ESQUEMA = "PRO"
        self.CX_CRN_ESQUEMA = "CRN"
        self.TL_PRO_TRANS_FOVISSSTE = "TRANS_FOV"
        self.TL_CRN_MATRIZ_CONV = "MATRIZ"


# helper that mirrors the core logic executed by the notebook
def run_notebook_logic(params, conf, query, db, logger):
    statement_extract = query.get_statement(
        "NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql",
        CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
        CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
        TL_TRANS_FOVISSSTE=conf.TL_PRO_TRANS_FOVISSSTE,
        TL_MATRIZ_CONVIV=conf.TL_CRN_MATRIZ_CONV,
        SR_FOLIO=params.sr_folio,
        SR_SUBPROCESO=params.sr_subproceso,
        SR_PROCESO=params.sr_proceso,
    )

    db.write_delta(
        f"RESULTADO_RECH_PROCESAR_{params.sr_folio}",
        db.read_data("default", statement_extract),
        "overwrite",
    )

    logger.info(f"✅ Registros rechazados FOVISSSTE extraídos - Folio: {params.sr_folio}")


# --- tests -----------------------------------------------------------

def test_query_parameters_are_passed():
    """When the notebook builds the SQL statement it should forward all fields."""
    params = DummyParams(sr_folio="F123", sr_subproceso="SP", sr_proceso="P")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert query.last_called is not None
    name, kwargs = query.last_called
    assert name == "NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql"
    # check a few of the expected keyword arguments
    assert kwargs["CX_PRO_ESQUEMA"] == "PRO"
    assert kwargs["CX_CRN_ESQUEMA"] == "CRN"
    assert kwargs["TL_TRANS_FOVISSSTE"] == "TRANS_FOV"
    assert kwargs["TL_MATRIZ_CONVIV"] == "MATRIZ"
    assert kwargs["SR_FOLIO"] == "F123"
    assert kwargs["SR_SUBPROCESO"] == "SP"
    assert kwargs["SR_PROCESO"] == "P"


def test_write_delta_is_called_with_expected_table_and_dataframe():
    """The notebook should write the dataframe returned by ``read_data`` under the
    correct table name and in overwrite mode.
    """
    params = DummyParams(sr_folio="ABC")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert len(db.written) == 1
    table_name, df, mode = db.written[0]
    assert table_name == "RESULTADO_RECH_PROCESAR_ABC"
    assert df == "DATA-default-STATEMENT(NB_PATRIF_REVMOV_0100_EXT_TRN_FOV_001_EXTRACT.sql)"
    assert mode == "overwrite"


def test_logger_receives_completion_message():
    """A friendly info log should mention the folio that was processed."""
    params = DummyParams(sr_folio="Z99")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert len(logger.messages) == 1
    assert "Z99" in logger.messages[0]
    assert "Registros rechazados FOVISSSTE extraídos" in logger.messages[0]


def test_empty_folio_still_constructs_table_name():
    """If the folio string is empty the table name should still be built without error."""
    params = DummyParams(sr_folio="")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert db.written[0][0] == "RESULTADO_RECH_PROCESAR_"
    # we don't care about the SQL here, just that the code executed normally
