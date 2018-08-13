import os
import sys
import configparser
import psycopg2
import csv
from functools import reduce

WINDOWS_ENCODING = '\\'
UNIX_ENCODING = '/'

def get_header(filepath):
    with open(filepath) as csv_file:
        return csv.DictReader(csv_file).fieldnames

def guess_type(header):
  if 'lon' in header or 'lat' in header:
    return 'float'
  if 'geom' in header:
    return 'geometry'
  if 'id' in header:
    return 'int'
  return 'string'

def separator():
    return WINDOWS_ENCODING if SYSTEM_TYPE == 'WINDOWS' else UNIX_ENCODING


def get_files():
    return [file for file in os.listdir(PATH)]


def import_csv(files):
    table_name = config['main']['EXISTING_TABLE_NAME']
    print("Batch Import Script.\nSystem Type: {}\nData: {}".format(SYSTEM_TYPE, files))
    if CREATE_NEW_TABLE:
        print("Create New Tables: {}".format(CREATE_NEW_TABLE))
    else:
        print("Add to Existing Table: {}".format(table_name))

    if input("Confirm Settings [Y/N]:").lower() != 'y':
        sys.exit()

    db = db_connection.cursor()

    for file in files:
        filepath = PATH + separator() + file
        if CREATE_NEW_TABLE:
            table_name = file.rsplit(".", 1)[0]  # Strip file extension from file

            create_table = reduce(lambda query, column_name: query + column_name + " " + guess_type(column_name) + ", ",
                                        get_header(filepath),
                                        "CREATE TABLE {} (".format(table_name))[:-2] + ");"

            print(create_table)
            print("STATUS: Creating new table", table_name)
            db.execute(create_table)
            print("STATUS: Finished creating new table", table_name)

        import_file = "COPY {} FROM '{}' DELIMITER ',' CSV HEADER;".format(table_name, filepath)
        print("STATUS: Beginning import of file", file)
        db.execute(import_file)
        print("STATUS: Finished import of file", file)

    db.close()


config = configparser.ConfigParser()
config.read('config.ini')

SYSTEM_TYPE = config['main']['SYSTEM_TYPE']
PATH = config['main']['DATA_LOCATION']
CREATE_NEW_TABLE = config['main'].getboolean('CREATE_NEW_TABLE')

dbname = config['db']['DB']
user = config['db']['USERNAME']
password = config['db']['PASSWORD']
port = config['db']['PORT']

db_connection = psycopg2.connect(dbname=dbname, user=user, password=password, port=port)
print("STATUS: Connected to database")

import_csv(get_files())
db_connection.commit()
print("STATUS: Committed to database")
db_connection.close()
print("STATUS: Connection to database closed")
