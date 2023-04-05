from Connection import Connection
from TimeETL import TimeETL

connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

time_etl = TimeETL(connection)
time_etl.extract('resources/DatosEjemplo.xlsx', 'Hoja1')
time_etl.transform()
time_etl.load()

connection.close()
