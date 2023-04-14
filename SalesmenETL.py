import random
import pandas
import unicodedata


class SalesmenETL:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def extract(self, name, sheet):
        self.dataframe = pandas.read_excel(name, sheet_name=sheet)

    def __normalize_columns(self):
        self.dataframe.columns = ['representative', 'region']

    def __generate_autoincremental_id(self):
        self.dataframe.reset_index(drop=True, inplace=True)
        self.dataframe['id'] = self.dataframe.index + 1

    def __generate_last_name(self):
        self.dataframe['last_name'] = self.dataframe['representative'].apply(lambda x: x.split()[1])

    def __generate_email(self):
        self.dataframe['email'] = (self.dataframe['representative'].str.replace(' ', '') + '@work.com').str.lower()

        # Replace the spanish characters.
        self.dataframe['email'] = self.dataframe['email'].apply(
            lambda x: unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8'))

        self.dataframe['email'] = self.dataframe['email'].str.replace('ñ', 'n')

    def __generate_contact_number(self):
        self.dataframe['contact_number'] = random.sample(range(10000000, 99999999), len(self.dataframe))

    def __generate_id_region(self):
        # Create a dictionary to map month numbers to month names in Spanish.
        id_region = {'Norte': 1, 'Sur': 2, 'Este': 3, 'Oeste': 4}

        # Apply the mapping to the 'region' column to get the id_region.
        self.dataframe['id_region'] = self.dataframe['region'].map(id_region)

    def get_dataframe(self):
        return self.dataframe

    def transform(self):
        self.__normalize_columns()
        self.__generate_autoincremental_id()
        self.__generate_id_region()
        self.__generate_last_name()
        self.__generate_email()
        self.__generate_contact_number()

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO salesmen (id, representative, region, id_region, last_name, email, contact_number) ' \
                    'VALUES (%s, \'%s\', \'%s\', %s, \'%s\', \'%s\', %s)' \
                    % (row[2], row[0], row[1], row[3], row[4], row[5], row[6])
            self.connection.execute(query)
