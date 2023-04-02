import pandas as pd
import mysql.connector
import sqlite3

class Connection:

    def __init__(self):
        self._my_db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Monstax514",
            database="project"
        )
    def create_tables(self):
        # Create the tables salesmen, sells, products, and time in MySQL.

        my_cursor = self._my_db.cursor()

        my_cursor.execute("CREATE TABLE salesmen (representative VARCHAR(255), region VARCHAR(255),  "
                          "first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), "
                          "contact_number VARCHAR(255), PRIMARY KEY(representative))")
        my_cursor.execute("CREATE TABLE products (product_code VARCHAR(255), description VARCHAR(255), price "
                          "INT, cost INT, PRIMARY KEY(product_code))")
        my_cursor.execute("CREATE TABLE time (date DATETIME, month INT, year INT, month_name VARCHAR(255), id INT"
                          ", PRIMARY KEY(id))")
        my_cursor.execute("CREATE TABLE sells (representative VARCHAR(255), product_code VARCHAR(255), units INT, "
                          "id_time INT NOT NULL, PRIMARY KEY(representative, product_code, id_time), "
                          "FOREIGN KEY (representative) REFERENCES salesmen(representative), "
                          "FOREIGN KEY (product_code) REFERENCES products(product_code), "
                          "FOREIGN KEY (id_time) REFERENCES time(id) "
                          "ON UPDATE CASCADE )")
        # Save the changes and close the connection to the database
        self._my_db.commit()

    def load_data(self, df_salesmen, df_products, df_time, df_sells):
        # Convert the DataFrame to a NumPy array
        data_salesmen = df_salesmen.values
        data_products = df_products.values
        data_time = df_time.values
        data_sells = df_sells.values

        my_cursor = self._my_db.cursor()

        # Iterate through each row of the NumPy array and insert each row into the MySQL table
        for row in data_salesmen:
            my_cursor.execute("INSERT INTO salesmen(representative, region, first_name, last_name, email, contact_number)"
                              " VALUES (%s, %s, %s, %s, %s, %s)", tuple(row))
        for row in data_products:
            my_cursor.execute("INSERT INTO products(product_code, description, price, cost) "
                              "VALUES (%s, %s, %s, %s)", tuple(row))
        for row in data_time:
            my_cursor.execute("INSERT INTO time(date, month, month_name, year, id) VALUES (%s, %s, %s, %s, %s)", tuple(row))
        for row in data_sells:
            my_cursor.execute("INSERT INTO sells(representative, product_code, units, id_time) VALUES (%s, %s, %s, %s)", tuple(row))

        # Save the changes and close the database connection
        self._my_db.commit()
        self._my_db.close()


