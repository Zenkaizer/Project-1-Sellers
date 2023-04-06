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

    def __generate_price(self):
        price = [random.randint(100, 999) for _ in range(len(self.dataframe))]
        self.dataframe['price'] = price

    def __generate_cost(self):
        self.dataframe['cost'] = (self.dataframe['price'] * 0.8).astype(int)

    def transform(self):
        self.__normalize_columns()
        self.__generate_price()
        self.__generate_cost()

    def get_dataframe(self):
        return self.dataframe

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO products (product_code, description, price, cost) ' \
                    'VALUES (\'%s\', \'%s\', %s, %s)' \
                    % (row[0], row[1], row[2], row[3])
            self.connection.execute(query)

