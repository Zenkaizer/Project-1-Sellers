from Connection import Connection
from ETL import ETL

etl = ETL()

# Extraction of data from Excel
table_sells = etl.extract("resources/DatosEjemplo.xlsx", "Hoja1")
table_salesmen = etl.extract("resources/DatosEjemplo.xlsx", "Hoja2")
table_products = etl.extract("resources/DatosEjemplo.xlsx", "Hoja3")

# Data transformation
table_time = etl.transform_table_time(table_sells)
table_salesmen = etl.transform_table_salesmen(table_salesmen)
table_products = etl.transform_table_products(table_products)
table_sells = etl.transform_table_sells(table_time, table_sells)

# Establishing connection with database
connection = Connection()

# Creating tables and loading data into MySQL
connection.create_tables()
connection.load_data(table_salesmen, table_products, table_time, table_sells)
