import csv

import pandas as pd
import sqlite3
from sqlite3 import Error
import yaml
import os

yaml_config_file = "/config.yml"

# Create a database connection to a SQLite database
def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        print("version:", sqlite3.sqlite_version)
    except Error as e:
        print(e)
    return connection


# Read configuration file
def read_codebook_yaml(codebook_filename):
    with open(codebook_filename, "r") as file:
        codebook = yaml.load(file, Loader=yaml.FullLoader)
    return codebook


# Function to import data into DB
def import_data_into_sql_lite(conn, filename, csv_data):
    # Create cursor with SQLite db connection
    cursor = conn.cursor()
    csv_data.to_sql(filename, conn, if_exists='replace', index=False)
    cursor.close()
    print(f"{filename}")


def detect_delimiter(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        sample = file.readline()
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=";,")
        except csv.Error:
            return ";"
        return delimiter.delimiter


# Get all files to import them into database
def retrieve_input_files(path, connect):
    # get all .csv filenames in export folder
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            # only use files with .csv filetype
            if filename.endswith('.csv'):
                # load momentApp csv data into Python
                delimiter = detect_delimiter(filepath)
                csv_data = pd.read_csv(filepath, delimiter=delimiter, engine='python')
                # load Momentapp csv data into SQL db
                filename = filename.replace(".csv", "")
                import_data_into_sql_lite(connect, filename, csv_data)


if __name__ == '__main__':

    # Read configuration file
    config = read_codebook_yaml("config.yaml")

    sql_lite_database_name = config['immerse_cleaned_ids']['maganamed_dvp_db']
    print("database path: ", sql_lite_database_name)

    # Create connection to SQLite database
    connect = create_connection(sql_lite_database_name)
    if connect is not None: print("Successful connection to SQLite database")

    file_directories = {
        'maganamed_path': config['immerse_cleaned_ids']['maganamed'],
        'movisens_esm_path': config['immerse_cleaned_ids']['movisens_esm'],
        'movisens_sensing_path': config['immerse_cleaned_ids']['movisens_sensing'],
        'dmmh_momentapp_path': config['immerse_cleaned_ids']['dmmh_momentapp'],
        'redcap_id_summary': config['immerse_cleaned_ids']['redcap_id_summary'], # TODO
    }

    key = list(file_directories.keys())
    print("file_directories: ", key)

    for key, data_directory in file_directories.items():
        print("Obtaining data from " + key)
        retrieve_input_files(data_directory, connect)

    if connect:
        # Finally commit db transaction/queries and close db connection
        connect.commit()
        connect.close()
