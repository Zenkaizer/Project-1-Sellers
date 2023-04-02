import mysql.connector


class Connect:

    @staticmethod
    def connect():
        connect = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='',
        )
        return connect

    @staticmethod
    def drop_table(cursor, table_name):
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
