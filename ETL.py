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
    def __generate_month_column(df):
        df['month'] = df['date'].dt.month

    @staticmethod
    def __generate_year_column(df):
        df['year'] = df['date'].dt.year

    @staticmethod
    def __generate_month_name(df):
        # Create a dictionary to map month numbers to month names in Spanish.
        months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August',
                  9: 'September', 10: 'October', 11: 'November', 12: 'December'}

        # Apply the mapping to the 'month' column to get the name of the month in Spanish.
        df['month_name'] = df['month'].map(months)

    @staticmethod
    def __separate_representative(df):
        # Split the column 'Representante' into two columns 'first_name' and 'last_name'.
        df[['first_name', 'last_name']] = df['representative'].str.split(' ', 1, expand=True)

    @staticmethod
    def __generate_email(df):
        # Create the column 'email' by concatenating 'first_name', 'last_name' and '@work.com'.
        df['email'] = (df['first_name'] + df['last_name'] + '@work.com').str.lower()

        # Replace the spanish characters.
        df['email'] = df['email'].apply(
            lambda x: unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8'))

        df['email'] = df['email'].str.replace('ñ', 'n')

    @staticmethod
    def __generate_phone_number(df):
        # Generate a random phone for each salesman
        phone_numbers = [str(random.randint(10000000, 99999999)) for _ in range(len(df))]
        df['contact_number'] = phone_numbers

    @staticmethod
    def __generate_price_cost(df, cost):
        # Generate a random price and production cost for each product
        price = [random.randint(100, 999) for _ in range(len(df))]
        df['price'] = price
        df['cost'] = (df['price'] * cost).astype(int)

    @staticmethod
    def __sum_equal_columns(df):
        # Sum up the units sold for the same salesmen, product, and date
        df_sum = df.groupby(['date', 'representative', 'product_code'], as_index=False).sum()
        return df_sum

    @staticmethod
    def __generate_id_time(df_time):
        # Remove duplicate rows based on the 'date' column
        df_time = df_time.drop_duplicates(subset=['date'])

        df_time['id'] = range(1, len(df_time) + 1)

        return df_time

    def transform_table_sells(self, df_time, df_sells):
        df_sells = self.__sum_equal_columns(df_sells)

        # An 'id_time' field is added to 'sells', which is a foreign key referencing the 'time' table
        df_sells['id_time'] = pd.merge(df_sells, df_time, on='date', how='left')['id']

        # The 'date' field is removed from the 'sells' table
        df_sells = df_sells.drop('date', axis=1)
        return df_sells

    def transform_table_salesmen(self, df):
        self.__separate_representative(df)
        self.__generate_email(df)
        self.__generate_phone_number(df)

        return df

    def transform_table_products(self, df):
        self.__generate_price_cost(df, 0.8)

        return df

    def transform_table_time(self, df):
        # Convert the date column to a date object.
        df_new = pd.DataFrame()
        df_new['date'] = pd.to_datetime(df['date'])

        # Extract the day, month and year in separate columns.
        self.__generate_month_column(df_new)
        self.__generate_month_name(df_new)
        self.__generate_year_column(df_new)
        self.__generate_day_column
        return self.__generate_id_time(df_new)
    @staticmethod
    def __generate_day_column(df):
        # Extract the day from each date and store it in a new column 'day'.
        df['day'] = df['date'].dt.day


