import random

import openpyxl
import pandas as pd
import unicodedata


class ETL:

    def __init__(self):
        pass

    @staticmethod
    def __normalize(df):
        normalize_columns = {'Fecha': 'date', 'Representante': 'representative', 'CódigoProducto': 'product_code',
                             'Unidades': 'units', 'Region': 'region', 'Descripción': 'description'}
        df = df.rename(columns=normalize_columns)
        return df

    def extract(self, name, sheet):
        df = pd.read_excel(name, sheet_name=sheet)
        return self.__normalize(df)

    @staticmethod
    def transform_table_time(df):
        # Convert the date column to a date object.
        df2 = pd.DataFrame()
        df2['date'] = pd.to_datetime(df['date'])

        # Extract the day, month and year in separate columns.
        df2['month'] = df['date'].dt.month
        df2['year'] = df['date'].dt.year

        # Create a dictionary to map month numbers to month names in Spanish.
        months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August',
                  9: 'September', 10: 'October', 11: 'November', 12: 'December'}

        # Apply the mapping to the 'Mes' column to get the name of the month in Spanish.
        df2['month_name'] = df2['month'].map(months)

        # Eliminamos las filas duplicadas basándonos en las columnas 'date'
        df2 = df2.drop_duplicates(subset=['date'])

        df2['id'] = range(1, len(df2) + 1)

        return df2

    @staticmethod
    def transform_table_salesmen(df):
        # Split the column 'Representante' into two columns 'first_name' and 'last_name'.
        df[['first_name', 'last_name']] = df['representative'].str.split(' ', 1, expand=True)

        # Create the column 'email' by concatenating 'first_name', 'last_name' and '@work.com'.
        df['email'] = (df['first_name'] + df['last_name'] + '@work.com').str.lower()

        # Replace the spanish characters.
        df['email'] = df['email'].apply(
            lambda x: unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8'))

        df['email'] = df['email'].str.replace('ñ', 'n')
        df = df.drop('first_name', axis=1)

        phone_numbers = [str(random.randint(10000000, 99999999)) for _ in range(len(df))]
        df['contact_number'] = phone_numbers

        return df

    @staticmethod
    def transform_table_products(df):
        price = [random.randint(100, 999) for _ in range(len(df))]
        df['price'] = price
        df['cost'] = (df['price'] * 0.8).astype(int)

        return df

    def transform_table_sells(self, df_time, df_sells ):

        df_sells['id_time'] = pd.merge(df_sells, df_time, on='date', how='left')['id']

        df_sells = df_sells.drop('date', axis=1)
        return df_sells

