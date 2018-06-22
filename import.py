import os
import sys
import configparser
import psycopg2

WINDOWS_ENCODING = '\\'
UNIX_ENCODING = '/'


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
        if CREATE_NEW_TABLE:
            table_name = file.rsplit(".", 1)[0]  # Strip file extension from file

            create_table = "CREATE TABLE {}" \
                           "(PROBE_ID character varying(255)," \
                           "SAMPLE_DATE timestamp without time zone," \
                           "LAT float,LON float," \
                           "HEADING float,SPEED float," \
                           "PROBE_DATA_PROVIDER character varying(255)," \
                           "SYSTEM_DATE timestamp without time zone);".format(table_name)

            print("STATUS: Creating new table", table_name)
            db.execute(create_table)
            print("STATUS: Finished creating new table", table_name)

        import_file = "COPY {} FROM '{}' DELIMITER ',' CSV HEADER;".format(table_name, PATH + separator() + file)
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
