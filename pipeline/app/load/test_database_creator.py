from pipeline.app.load import database_creator as dc

dc.cursor = dc.connect_to_database('localhost,1433', 'SA', 'Passw0rd2018', 'master').cursor()
cursor = dc.cursor

# tests if the database has been created
def test_database_create():
    assert type(dc.create_database(dc.cursor, 'Data24ETL')) is bool
    assert dc.create_database(dc.cursor, 'Data24ETL') is False

# tests the existance of the created database
def test_check_db_exists():
    assert type(dc.check_db_exists(dc.cursor, 'Data24ETL')) is bool
    assert dc.check_db_exists(dc.cursor, 'Data24ETL') is True

# tests the database reset function
def test_reset_database():
    assert type(dc.reset_database(dc.cursor, 'Data24ETL')) is bool
    assert dc.reset_database(dc.cursor, 'Data24ETL') is True




