import pandas


class SellsETL:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def extract(self, name, sheet):
        self.dataframe = pandas.read_excel(name, sheet_name=sheet)

    def __normalize_columns(self):
        self.dataframe.columns = ['date', 'representative', 'product_code', 'units']
        self.dataframe.reindex(columns=['representative', 'product_code', 'date', 'units'])

    def __sum_equal_columns(self):
        # Sum up the units sold for the same salesmen, product, and date
        self.dataframe = self.dataframe.groupby(['date', 'representative', 'product_code'], as_index=False).sum()

    def __generate_profits(self, df_products):
        # Merge the dataframes in the "product_code" column
        df_merged = pandas.merge(self.dataframe, df_products, on='product_code', how='left')

        # Calculate the "profit" column as the subtraction between the "price" column and the "cost" column
        df_merged['profits'] = df_merged['price'] - df_merged['cost']
        self.dataframe = df_merged

    def transform(self, df_products):
        self.__normalize_columns()
        self.__sum_equal_columns()
        self.__generate_profits(df_products)

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO sells (representative, product_code, date, units, profits) ' \
                    'VALUES (\'%s\', %s, \'%s\', %s, \'%s\')' \
                    % (row[0], row[1], row[2], row[3], row[4])
            self.connection.execute(query)
