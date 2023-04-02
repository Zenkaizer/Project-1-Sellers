from ETL import ETL
import openpyxl
import pandas as pd

etl = ETL()


table1 = etl.transform_table1(etl.extract("DatosEjemplo.xlsx", "Hoja1"))
table2 = etl.transform_table2(etl.extract("DatosEjemplo.xlsx", "Hoja2"))
table3 = etl.transform_table3(etl.extract("DatosEjemplo.xlsx", "Hoja3"))
table4 = etl.transform_table4(table1)


table1.to_excel('ejemplo.xlsx', index=False)

"""

etl.load_table1(table1)
etl.load_table2(table2)
etl.load_table3(table3)
etl.load_table4(table4)

"""
