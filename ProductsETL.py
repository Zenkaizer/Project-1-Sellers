import random

import pandas


class ProductsETL:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def extract(self, name, sheet):
        self.dataframe = pandas.read_excel(name, sheet_name=sheet)

    def __normalize_columns(self):
        self.dataframe.columns = ['product_code', 'description']

    def __generate_autoincremental_id(self):
        self.dataframe.reset_index(drop=True, inplace=True)
        self.dataframe['id'] = self.dataframe.index + 1

    def __generate_price(self):
        price = [random.randint(100, 999) for _ in range(len(self.dataframe))]
        self.dataframe['price'] = price

    def __generate_cost(self):
        self.dataframe['cost'] = (self.dataframe['price'] * 0.8).astype(int)

    def get_dataframe(self):
        return self.dataframe

    def transform(self):
        self.__normalize_columns()
        self.__generate_autoincremental_id()
        self.__generate_price()
        self.__generate_cost()

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO products (id, product_code, description, price, cost) ' \
                    'VALUES (%s, \'%s\', \'%s\', %s, %s)' \
                    % (row[2], row[0], row[1], row[3], row[4])
            self.connection.execute(query)

