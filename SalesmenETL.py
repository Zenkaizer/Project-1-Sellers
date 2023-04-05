import random
import pandas
import unicodedata


class SalesmenETL:

    def __init__(self):
        self.df_salesmen = self.extract("DatosEjemplo.xlsx", "Hoja2")

    def extract(self, name, sheet):
        self.df_salesmen = pandas.read_excel(name, sheet_name=sheet)
        return self.df_salesmen

    def __normalize_columns(self):
        self.df_salesmen.columns = ['representative', 'region']

    def __generate_last_name(self):
        self.df_salesmen[['first_name', 'last_name']] = self.df_salesmen['representative'].str.split(' ', 1,
                                                                                                     expand=True)

    def __generate_email(self):
        self.df_salesmen['email'] = (
                self.df_salesmen['first_name'] + self.df_salesmen['last_name'] + '@work.com').str.lower()

        # Replace the spanish characters.
        self.df_salesmen['email'] = self.df_salesmen['email'].apply(
            lambda x: unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8'))

        self.df_salesmen['email'] = self.df_salesmen['email'].str.replace('ñ', 'n')

    def __generate_contact_number(self):
        phone_numbers = [str(random.randint(10000000, 99999999)) for _ in range(len(self.df_salesmen))]
        self.df_salesmen['contact_number'] = phone_numbers

    def __generate_id_region(self):
        # Create a dictionary to map month numbers to month names in Spanish.
        id_region = {'Norte': 1, 'Sur': 2, 'Este': 3, 'Oeste': 4}

        # Apply the mapping to the 'region' column to get the id_region.
        self.df_salesmen['id_region'] = self.df_salesmen['region'].map(id_region)

    def transform(self):
        self.__generate_id_region()
        self.__generate_last_name()
        self.__generate_email()
        self.__generate_contact_number()
        self.df_salesmen.drop('first_name', axis=1)

    # def load(self):
