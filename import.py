import os
import sys
import configparser
import psycopg2
import csv
import pprint
from functools import reduce

WINDOWS_ENCODING = '\\'
UNIX_ENCODING = '/'

def get_header(filepath):
    with open(filepath) as csv_file:
        return csv.DictReader(csv_file).fieldnames

def guess_type(header):
  if 'lon' in header or 'lat' in header or 'score' in header:
    return 'float'
  if 'geom' in header:
    return 'geometry'
  if 'id' in header:
    return 'int'
  return 'varchar'

def separator():
    return WINDOWS_ENCODING if SYSTEM_TYPE == 'WINDOWS' else UNIX_ENCODING


def get_files():
    return [file for file in os.listdir(PATH)]


def import_csv(files):
    table_name = config['main']['EXISTING_TABLE_NAME']
    print('Import table(s):')
    pprint.PrettyPrinter(indent=2).pprint(files)

    if CREATE_NEW_TABLE:
        print("by creating new tables: {}".format(CREATE_NEW_TABLE))
    else:
        print("by adding to existing table: {}".format(table_name))

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
            try:
              db.execute(create_table)
              print("\tSTATUS: Creating new table", table_name)
            except:
              print('\tWARNING: Unable to create table', table_name)
              db_connection.rollback()
        import_file = "COPY {} FROM '{}' DELIMITER ',' CSV HEADER;".format(table_name, filepath)
        try:
          db.execute(import_file)
          print("\tSTATUS: Importing file", file)
        except:
          print('\tWARNING: Unable to import file', file)
          db_connection.rollback()

    db.close()

def delete_tables(files):
  print('Delete table(s):')
  pprint.PrettyPrinter(indent=2).pprint(files)
  if (input('Confirm Settings [Y/N]:').lower() != 'y'):
    sys.exit()
  
  db = db_connection.cursor()

  for file in files:
    table_name = file.rsplit('.', 1)[0]  # Strip file extension from file
    delete_table = 'DROP TABLE {}'.format(table_name)
    try:
      db.execute(delete_table)
      print('\tSTATUS: Deleted table {}'.format(table_name))
    except psycopg2.ProgrammingError:
      print('\tWARNING: Unable to delete non-existent table {}'.format(table_name))
      db_connection.rollback()

  db.close()


config = configparser.ConfigParser()
config.read('config.ini')

SYSTEM_TYPE = config['main']['SYSTEM_TYPE']
PATH = config['main']['DATA_LOCATION']
DELETE_TABLES = config['main'].getboolean('DELETE_TABLES')
IMPORT_TABLES = config['main'].getboolean('IMPORT_TABLES')
CREATE_NEW_TABLE = config['main'].getboolean('CREATE_NEW_TABLE')

dbname = config['db']['DB']
user = config['db']['USERNAME']
password = config['db']['PASSWORD']
port = config['db']['PORT']

db_connection = psycopg2.connect(dbname=dbname, user=user, password=password, port=port)
print('STATUS: Connected to database')

if DELETE_TABLES:
  delete_tables(get_files())

if IMPORT_TABLES:
  import_csv(get_files())

db_connection.commit()
print('STATUS: Committed to database')
db_connection.close()
print('STATUS: Connection to database closed')
