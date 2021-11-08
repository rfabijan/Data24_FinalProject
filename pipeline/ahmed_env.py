from pipeline.app.load.database_creator import *
from pipeline.app.load.insert_data import *
from pprint import pprint as pp

connection = connect_to_database('localhost,1433', 'SA', 'Passw0rd2018', 'master')
cursor = connection.cursor()

print("Building Database using Script")
run_script(cursor, 'app/load/database_creator.sql')
insert_into_academy(cursor, db_name='Data24ETL', values="('London'), ('Birmingham'), ('Leicester')")
# insert_into_academy(cursor, db_name='Data24ETL', values=['London', 'Birmingham', 'Leicester'])

cursor.execute('SELECT * FROM Academy')
pp(cursor.fetchall())
connection.close()
