-- Create database
DROP DATABASE IF EXISTS my_database;
CREATE DATABASE my_database;

-- Connect to the newly created database
\c my_database;

-- Create table
DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (
    id SERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    gender      TEXT,
    birth_date  DATE
);

-- Insert data into the table
INSERT INTO my_table (first_name, last_name, gender, birth_date) VALUES ('John', 'Doe', 'M', '1990-03-01');
INSERT INTO my_table (first_name, last_name, gender, birth_date) VALUES ('Annah', 'Williams', 'F', '1980-08-15');