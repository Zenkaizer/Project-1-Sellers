import pandas


class TimeETL:
    def __init__(self, connection):
        """

        :param connection:
        """
        self.dataframe = None
        self.connection = connection

    def extract(self, name, sheet):
        self.dataframe = pandas.read_excel(name, sheet_name=sheet)

    def __normalize_columns(self):
        self.dataframe.columns = ['date', 'representative', 'product_code', 'units']
        self.dataframe.drop(columns=['representative', 'product_code', 'units'], inplace=True)

    def __group_equal_columns(self):
        self.dataframe.drop_duplicates(inplace=True)

    def __generate_month_number(self):
        self.dataframe['month_number'] = self.dataframe['date'].dt.month

    def __generate_month_name(self):
        months = {
            1: 'Enero',
            2: 'Febrero',
            3: 'Marzo',
            4: 'Abril',
            5: 'Mayo',
            6: 'Junio',
            7: 'Julio',
            8: 'Agosto',
            9: 'Septiembre',
            10: 'Octubre',
            11: 'Noviembre',
            12: 'Diciembre'
        }

        self.dataframe['month_name'] = self.dataframe['month_number'].map(months)

    def __generate_year(self):
        self.dataframe['year'] = self.dataframe['date'].dt.year

    def __generate_day(self):
        self.dataframe['day'] = self.dataframe['date'].dt.day

    def transform(self):
        self.__normalize_columns()
        self.__group_equal_columns()
        self.__generate_month_number()
        self.__generate_month_name()
        self.__generate_year()
        self.__generate_day()

    def load(self):
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO time (date, month_number, month_name, year, day) VALUES (\'%s\', %s, \'%s\', %s, %s)'\
                    % (row[0].strftime('%Y-%m-%d'), row[1], row[2], row[3], row[4])
            self.connection.execute(query)
