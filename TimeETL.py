import pandas


class TimeETL:
    def __init__(self, connection):
        """
        TimeETL class constructor
        :param connection: Connection to MySQL database
        """
        self.dataframe = None
        self.connection = connection

    def extract(self, path, sheet):
        """
        Read an Excel file for ETL process from time table
        :param path: Excel file location
        :param sheet: Excel file sheet name
        :return: None
        """
        self.dataframe = pandas.read_excel(path, sheet_name=sheet)

    def __normalize_columns(self):
        """
        Rename the columns and removes that ones that are not used
        :return: None
        """
        self.dataframe.columns = ['date', 'representative', 'product_code', 'units']
        self.dataframe.drop(columns=['representative', 'product_code', 'units'], inplace=True)

    def __generate_autoincremental_id(self):
        self.dataframe.reset_index(drop=True, inplace=True)
        self.dataframe['id'] = self.dataframe.index + 1

    def __group_equal_columns(self):
        """
        Group only unique dates
        :return: None
        """
        self.dataframe.drop_duplicates(inplace=True)

    def __generate_month_number(self):
        """
        Create a column to store the month number from the date column
        :return: None
        """
        self.dataframe['month_number'] = self.dataframe['date'].dt.month

    def __generate_month_name(self):
        """
        Create a column to store the month name from the month number column
        :return: None
        """
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
        """
        Create a column to store the year from the date column
        :return: None
        """
        self.dataframe['year'] = self.dataframe['date'].dt.year

    def __generate_day(self):
        """
        Create a column to store the day from the date column
        :return: None
        """
        self.dataframe['day'] = self.dataframe['date'].dt.day

    def get_dataframe(self):
        return self.dataframe

    def transform(self):
        """
        Execute all the transformations for the time table
        :return: None
        """
        self.__normalize_columns()
        self.__group_equal_columns()
        self.__generate_month_number()
        self.__generate_month_name()
        self.__generate_year()
        self.__generate_day()
        self.__generate_autoincremental_id()

    def load(self):
        """
        Load in time table
        :return: None
        """
        for row in self.dataframe.to_numpy():
            query = 'INSERT INTO time (date, month_number, month_name, year, day) VALUES (\'%s\', %s, \'%s\', %s, %s)'\
                    % (row[0], row[1], row[2], row[3], row[4])
            self.connection.execute(query)
