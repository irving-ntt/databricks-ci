import pytest


class DummyQuery:
    def __init__(self):
        self.last_called = None

    def get_statement(self, name, **kwargs):
        self.last_called = (name, kwargs)
        return f"STMT({name})"


class DummyDB:
    def __init__(self):
        self.written = []

    def sql_delta(self, statement):
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


# replicate notebook logic

def run_notebook_logic(params, query, db, logger):
    statement_sumario = query.get_statement(
        "NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql",
    )

    db.write_delta(
        f"RESULTADO_SUMARIO_INTEGRITY_{params.sr_folio}",
        db.sql_delta(statement_sumario),
        "overwrite",
    )

    logger.info("Registro sumario Integrity generado exitosamente")


# --- tests -----------------------------------------------------------

def test_statement_is_requested_without_kwargs():
    params = DummyParams(sr_folio="FOLIO")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, query, db, logger)

    assert query.last_called is not None
    name, kwargs = query.last_called
    assert name == "NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql"
    assert kwargs == {}


def test_write_delta_and_table_name():
    params = DummyParams(sr_folio="123")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, query, db, logger)

    assert len(db.written) == 1
    table, df, mode = db.written[0]
    assert table == "RESULTADO_SUMARIO_INTEGRITY_123"
    assert df == "SQLDATA-STMT(NB_PATRIF_REVMOV_0200_DESM_NCI_005_SUMARIO.sql)"
    assert mode == "overwrite"


def test_logger_message_present():
    params = DummyParams(sr_folio="X")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, query, db, logger)

    assert len(logger.messages) == 1
    assert "Registro sumario Integrity generado exitosamente" in logger.messages[0]


def test_empty_folio_still_works():
    params = DummyParams(sr_folio="")
    query = DummyQuery()
    db = DummyDB()
    logger = DummyLogger()

    run_notebook_logic(params, query, db, logger)

    assert db.written[0][0] == "RESULTADO_SUMARIO_INTEGRITY_"
