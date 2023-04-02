import pandas as pd
import mysql.connector
import sqlite3

class Connection:

    def __init__(self):
        self.my_db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Monstax514",
            database="project"
        )
    def create_tables(self):

        my_cursor = self.my_db.cursor()
        my_cursor.execute("CREATE TABLE salesmen (representative VARCHAR(255), region VARCHAR(255),  "
                          "lastname VARCHAR(255), email VARCHAR(255), contact_number VARCHAR(255)"
                          ", PRIMARY KEY(representative))")
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
        # Guarda los cambios y cierra la conexión a la base de datos
        self.my_db.commit()

    def load_data(self, df_salesmen, df_products, df_time, df_sells):
        # Convierte el DataFrame a una matriz NumPy
        data_salesmen = df_salesmen.values
        data_products = df_products.values
        data_time = df_time.values
        data_sells = df_sells.values

        my_cursor = self.my_db.cursor()

        # Itera a través de cada fila de la matriz NumPy y inserta cada fila en la tabla MySQL
        for row in data_salesmen:
            my_cursor.execute("INSERT INTO salesmen(representative, region, lastname, email, contact_number)"
                              " VALUES (%s, %s, %s, %s, %s)", tuple(row))
        for row in data_products:
            my_cursor.execute("INSERT INTO products(product_code, description, price, cost) "
                              "VALUES (%s, %s, %s, %s)", tuple(row))
        for row in data_time:
            my_cursor.execute("INSERT INTO time(date, month, year, month_name, id) VALUES (%s, %s, %s, %s, %s)", tuple(row))
        for row in data_sells:
            my_cursor.execute("INSERT INTO sells(representative, product_code, units, id_time) VALUES (%s, %s, %s, %s)", tuple(row))

        # Guarda los cambios y cierra la conexión a la base de datos
        self.my_db.commit()
        self.my_db.close()


