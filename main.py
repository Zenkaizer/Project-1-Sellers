from ETL import ETL
import openpyxl
import pandas as pd
from Connection import Connection

etl = ETL()

table_sells = etl.extract("DatosEjemplo.xlsx", "Hoja1")
table_salesmen = etl.extract("DatosEjemplo.xlsx", "Hoja2")
table_products = etl.extract("DatosEjemplo.xlsx", "Hoja3")

table_time = etl.transform_table_time(table_sells)
table_salesmen = etl.transform_table_salesmen(table_salesmen)
table_products = etl.transform_table_products(table_products)

table_sells = etl.transform_table_sells(table_time, table_sells)

connection = Connection()
connection.create_tables()
connection.load_data(table_salesmen, table_products, table_time, table_sells)




