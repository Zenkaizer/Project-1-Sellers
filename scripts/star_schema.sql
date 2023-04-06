DROP TABLE IF EXISTS sells CASCADE;
DROP TABLE IF EXISTS salesmen CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS time CASCADE;

CREATE TABLE salesmen (
  id             int(10) NOT NULL AUTO_INCREMENT,
  representative varchar(255) NOT NULL UNIQUE,
  region         varchar(255) NOT NULL,
  id_region      int(10) NOT NULL,
  last_name      varchar(255) NOT NULL,
  email          varchar(255) NOT NULL UNIQUE,
  contact_number int(10) NOT NULL,
  PRIMARY KEY (id));
CREATE TABLE products (
  id           int(10) NOT NULL AUTO_INCREMENT,
  product_code varchar(255) NOT NULL UNIQUE,
  description  varchar(255) NOT NULL,
  price        int(10) NOT NULL,
  cost         int(10) NOT NULL,
  PRIMARY KEY (id));
CREATE TABLE time (
  id           int(10) NOT NULL AUTO_INCREMENT,
  `date`       date NOT NULL UNIQUE,
  month_number int(10) NOT NULL,
  month_name   varchar(255) NOT NULL,
  year         int(10) NOT NULL,
  day          int(10) NOT NULL,
  PRIMARY KEY (id));
CREATE TABLE sells (
  id          int(10) NOT NULL AUTO_INCREMENT,
  salesmen_id int(10) NOT NULL,
  products_id int(10) NOT NULL,
  time_id     int(10) NOT NULL,
  units       int(10) NOT NULL,
  profits     int(10) NOT NULL,
  PRIMARY KEY (id));
ALTER TABLE sells ADD CONSTRAINT FKsells33993 FOREIGN KEY (salesmen_id) REFERENCES salesmen (id);
ALTER TABLE sells ADD CONSTRAINT FKsells815083 FOREIGN KEY (products_id) REFERENCES products (id);
ALTER TABLE sells ADD CONSTRAINT FKsells43867 FOREIGN KEY (time_id) REFERENCES time (id);