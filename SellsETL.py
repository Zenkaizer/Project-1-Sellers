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
        df_merged = pandas.merge(self.dataframe, df_products[['product_code', 'price', 'cost']], on='product_code', how='left')

        # Calculate the "profit" column as the subtraction between the "price" column and the "cost" column
        df_merged['profits'] = df_merged['price'] - df_merged['cost']
        df_merged.drop('price', axis=1, inplace=True)
        df_merged.drop('cost', axis=1, inplace=True)
        self.dataframe = df_merged

    def __sync_id_time(self, df_time):
        # Merge para obtener la columna 'salesmen_id'
        self.dataframe = pandas.merge(self.dataframe, df_time[['date', 'id']], on='date', how='left')
        self.dataframe.rename(columns={'id': 'time_id'}, inplace=True)

        self.dataframe.drop('date', axis=1, inplace=True)

    def __sync_id_products(self, df_products):
        # Merge para obtener la columna 'salesmen_id'
        self.dataframe = pandas.merge(self.dataframe, df_products[['product_code', 'id']], on='product_code', how='left')
        self.dataframe.rename(columns={'id': 'products_id'}, inplace=True)

        # Eliminar la columna 'representante'
        self.dataframe.drop('product_code', axis=1, inplace=True)

    def __sync_id_salesmen(self, df_salesmen):
        # Merge para obtener la columna 'salesmen_id'
        self.dataframe = pandas.merge(self.dataframe, df_salesmen[['representative', 'id']], on='representative', how='left')
        self.dataframe.rename(columns={'id': 'salesmen_id'}, inplace=True)

        # Eliminar la columna 'representante'
        self.dataframe.drop('representative', axis=1, inplace=True)

    def transform(self, df_products, df_salesmen, df_time):
        self.__normalize_columns()
        self.__sum_equal_columns()
        self.__generate_profits(df_products)
        self.__sync_id_products(df_products)
        self.__sync_id_salesmen(df_salesmen)
        self.__sync_id_time(df_time)

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO sells (salesmen_id, products_id, time_id, units, profits) ' \
                    'VALUES (\'%s\', %s, \'%s\', %s, \'%s\')' \
                    % (row[3], row[2], row[4], row[0], row[1])
            self.connection.execute(query)
