import random

import pandas


class ProductsETL:

    def __init__(self):
        self.df_products = self.extract("DatosEjemplo.xlsx", "Hoja3")

    def extract(self, name, sheet):
        self.df_products = pandas.read_excel(name, sheet_name=sheet)
        return self.df_products

    def __normalize_columns(self):
        self.df_products.columns = ['product_code', 'description']

    def __generate_price(self):
        price = [random.randint(100, 999) for _ in range(len(self.df_products))]
        self.df_products['price'] = price

    def __generate_cost(self):
        self.df_products['cost'] = (self.df_products['price'] * self.df_products).astype(int)

    def transform(self):
        self.__normalize_columns()
        self.__generate_price()
        self.__generate_cost()

# def load(self):
