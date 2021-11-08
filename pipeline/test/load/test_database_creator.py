from pipeline.app.load import database_creator as dc

dc.cursor = dc.connect_to_database('localhost,1433', 'SA', 'Passw0rd2018', 'master').cursor()

def test_database_exists():
    assert type(dc.create_database(dc.cursor, 'Data24ETL')) is bool
    assert dc.create_database(dc.cursor, 'Data24ETL') is False


def test_check_db_exists():
    assert type(dc.check_db_exists(dc.cursor, 'Data24ETL')) is bool
    assert dc.check_db_exists(dc.cursor, 'Data24ETL') is True


def test_reset_database():
    assert type(dc.reset_database(dc.cursor, 'Data24ETL')) is bool
    assert dc.reset_database(dc.cursor, 'Data24ETL') is True


