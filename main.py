from Connection import Connection
from ProductsETL import ProductsETL
from SalesmenETL import SalesmenETL
from SellsETL import SellsETL
from TimeETL import TimeETL

connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

time_etl = TimeETL(connection)
time_etl.extract('resources/DatosEjemplo.xlsx', 'Hoja1')
time_etl.transform()
time_etl.load()

salesmen_etl = SalesmenETL(connection)
salesmen_etl.extract('resources/DatosEjemplo.xlsx', 'Hoja2')
salesmen_etl.transform()
salesmen_etl.load()

products_etl = ProductsETL(connection)
products_etl.extract('resources/DatosEjemplo.xlsx', 'Hoja3')
products_etl.transform()
products_etl.load()

sells_etl = SellsETL(connection)
sells_etl.extract('resources/DatosEjemplo.xlsx', 'Hoja1')
sells_etl.transform(products_etl.get_dataframe(), salesmen_etl.get_dataframe(), time_etl.get_dataframe())
sells_etl.load()

connection.close()
