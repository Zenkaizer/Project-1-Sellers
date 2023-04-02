from ETL import ETL
import openpyxl
import pandas as pd

etl = ETL()

table1 = etl.transform_table1(etl.extract("DatosEjemplo.xlsx", "Hoja1"))
table2 = etl.transform_table2(etl.extract("DatosEjemplo.xlsx", "Hoja2"))
table3 = etl.transform_table3(etl.extract("DatosEjemplo.xlsx", "Hoja3"))
table4 = etl.transform_table4(table1)

