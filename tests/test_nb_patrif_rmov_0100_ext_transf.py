import pytest


class DummyQuery:
    def __init__(self):
        self.last_called = None

    def get_statement(self, name, **kwargs):
        self.last_called = (name, kwargs)
        return f"SQL({name})"


class DummyDB:
    def __init__(self):
        self.writes = []

    def read_data(self, database, statement):
        return f"DATA-{database}-{statement}"

    def write_delta(self, table_name, df, mode):
        self.writes.append((table_name, df, mode))


class DummyLogger:
    def __init__(self):
        self.msgs = []

    def info(self, msg):
        self.msgs.append(msg)


class DummyParams:
    def __init__(self, folio, subproceso=None, proceso=None):
        self.sr_folio = folio
        self.sr_subproceso = subproceso or "SP"
        self.sr_proceso = proceso or "P"


class DummyConf:
    def __init__(self):
        self.CX_PRO_ESQUEMA = "PRO"
        self.CX_CRN_ESQUEMA = "CRN"
        self.TL_PRO_TRANS_INFONA = "TRANS"
        self.TL_CRN_MATRIZ_CONV = "MATRIZ"
        self.TL_CRN_IND_CTA_INDV = "IND"


# replicate notebook logic

def run_notebook_logic(params, conf, query, db, logger):
    statement_extract = query.get_statement(
        "NB_PATRIF_RMOV_0100_EXT_TRANSF_001_EXTRACT.sql",
        CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
        CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
        TL_TRANS_INFONA=conf.TL_PRO_TRANS_INFONA,
        TL_MATRIZ_CONVIV=conf.TL_CRN_MATRIZ_CONV,
        TL_IND_CTA_INDV=conf.TL_CRN_IND_CTA_INDV,
        SR_FOLIO=params.sr_folio,
        SR_SUBPROCESO=params.sr_subproceso,
        SR_PROCESO=params.sr_proceso,
    )

    db.write_delta(
        f"RESULTADO_RECH_PROCESAR_{params.sr_folio}",
        db.read_data("default", statement_extract),
        "overwrite",
    )


# tests --------------------------------------------------------

def test_query_params_forwarded():
    params = DummyParams(folio="F1", subproceso="X", proceso="Y")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert query.last_called is not None
    name, kwargs = query.last_called
    assert name == "NB_PATRIF_RMOV_0100_EXT_TRANSF_001_EXTRACT.sql"
    assert kwargs["CX_PRO_ESQUEMA"] == "PRO"
    assert kwargs["CX_CRN_ESQUEMA"] == "CRN"
    assert kwargs["TL_TRANS_INFONA"] == "TRANS"
    assert kwargs["TL_MATRIZ_CONVIV"] == "MATRIZ"
    assert kwargs["TL_IND_CTA_INDV"] == "IND"
    assert kwargs["SR_FOLIO"] == "F1"
    assert kwargs["SR_SUBPROCESO"] == "X"
    assert kwargs["SR_PROCESO"] == "Y"


def test_write_delta_and_table_name():
    params = DummyParams(folio="ABC")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)

    assert len(db.writes) == 1
    tab, df, mode = db.writes[0]
    assert tab == "RESULTADO_RECH_PROCESAR_ABC"
    assert df == "DATA-default-SQL(NB_PATRIF_RMOV_0100_EXT_TRANSF_001_EXTRACT.sql)"
    assert mode == "overwrite"


def test_empty_folio_creates_table():
    params = DummyParams(folio="")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)
    assert db.writes[0][0] == "RESULTADO_RECH_PROCESAR_"


def test_logger_not_used():
    # notebook has no logger.info call; ensure absence
    params = DummyParams(folio="Z")
    conf = DummyConf()
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, conf, query, db, logger)
    assert logger.msgs == []
