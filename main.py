import pandas as pd
import sqlite3
from sqlite3 import Error
import yaml
import os


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


# TODO: OBTAIN Maganamed data (pending)
def get_maganamed_ecrf_target(Dictionary, eCRF_filename):
    """ scan through yaml file to find eCRF target and return index"""
    # Questionnarie daten (Maganamed)
    for key1 in Dictionary.keys():
        item = {key1: key2 for key2 in Dictionary[key1].keys() if eCRF_filename in Dictionary[key1][key2].values()}
        if len(item) > 0:
            return item


# Create string for sql query to create table and define columns with data types
def create_data_types_string_for_query(codebook, eCRF_filename):
    print(eCRF_filename)

    # get target eCRF index in codebook.yaml file
    eCRF_search_result = get_maganamed_ecrf_target(codebook, eCRF_filename)['eCRFs']

    # first seven columns which are identical in most Maganamed eCRFs
    common_first_seven_columns = '''participant_identifier TEXT,
                        center_name TEXT,
                        created_at TEXT,
                        started_at TEXT,
                        finished_at TEXT,
                        visit_name TEXT,
                        diary_date TEXT,'''

    types_string = common_first_seven_columns
    # get item name and data type for all items in target eCRF and return results as string for query
    for codebook_item in codebook['eCRFs'][eCRF_search_result]['items']:
        # get itemDataType for one item in target eCRF
        data_type = codebook['eCRFs'][eCRF_search_result]['items'][codebook_item]['itemDataType']
        # get itemCode for one item in target eCRF
        item_name = codebook['eCRFs'][eCRF_search_result]['items'][codebook_item]['itemCode']

        # Convert codebook.yaml data type to SQL data type; possible data types in codebook.yaml: date, text, singleChoice, number, multipleChoice
        if data_type != 'number':
            data_type = 'TEXT'
        else:
            data_type = 'INTEGER'
        # concatenate string for all items in target eCRF
        types_string = types_string + '`' + item_name + ' ' + data_type + '`,'
    # Exchange last comma with the definition for the primary key of the new table
    # The primary key needs to be unique in the whole table; therefore participant_identifier alone was not suitable
    types_string = types_string[:-1] + ', PRIMARY KEY (participant_identifier, visit_name)'
    return types_string


def create_data_types_string_for_query_participants(eCRF_filename):
    """" create string for sql query to create table and define columns with data types """
    print(eCRF_filename)

    # define table structure
    eCRF_participants_columns = '''participant_identifier TEXT,
                        survey_identifier TEXT,
                        center_name TEXT,
                        PRIMARY KEY (participant_identifier)'''

    return eCRF_participants_columns


def create_data_types_string_for_query_study(eCRF_filename):
    """" create string for sql query to create table and define columns with data types """
    print(eCRF_filename)

    # first four columns which are identical in these two files
    eCRF_first_four_columns = '''center_name TEXT,
                        participant_identifier TEXT,
                        visit_name TEXT,
                        form_name TEXT'''

    final_string_for_query = eCRF_first_four_columns

    # different logic to build table for these two different files; specified manually because file is not included
    # in codebook.yaml
    if eCRF_filename == "study-participant-forms.csv":
        final_string_for_query = final_string_for_query + ''',deleted_at TEXT,
                        locked_at TEXT,
                        Remote_Verification_at TEXT,
                        signature_at TEXT,
                        Remote_Verification_history TEXT,
                        signature_history TEXT,
                        PRIMARY KEY (participant_identifier, visit_name, form_name)'''

    if eCRF_filename == "study-queries.csv":
        final_string_for_query = final_string_for_query + ''',form_item TEXT,
                        created_at TEXT,
                        status TEXT,
                        history TEXT,
                        PRIMARY KEY (participant_identifier, created_at)'''

    return final_string_for_query


def import_maganamed_data_into_sqllite(conn, eCRF_filename, csv_data, codebook):
    # Create cursor with SQLite db connection
    cursor = conn.cursor()

    # reformat eCRF filename to allow the usage as db table name
    eCRF_name_for_query = eCRF_filename.rsplit('.csv')[0].replace('-', '_').replace('(', '').replace(')', '')

    # # create final query string for special - not eCRF - files
    if eCRF_filename == "participants.csv":
        data_types_string = create_data_types_string_for_query_participants(eCRF_filename)
    elif eCRF_filename == "study-participant-forms.csv":
        data_types_string = create_data_types_string_for_query_study(eCRF_filename)
    elif eCRF_filename == "study-queries.csv":
        data_types_string = create_data_types_string_for_query_study(eCRF_filename)
    # create second part of the sql query to create table and define columns with data types
    else:
        data_types_string = create_data_types_string_for_query(codebook, eCRF_filename)

    # create final query string
    create_table_query = 'CREATE TABLE IF NOT EXISTS ' + eCRF_name_for_query + ' (' + data_types_string + ')'
    # execute SQL query to create table
    cursor.execute(create_table_query)

    # for loop to iterate through rows
    for index, row in csv_data.iterrows():
        # do not use rows with test data
        if row['center_name'] != 'main':
            # replace ' with " in all rows to circumvent problems with the SQL query
            # Anita: In the UK the study team experiences difficulties when sending out links to the questionnaires in Maganamed
            # in case a MaganaMed log-in code has been created. This is expecially the case when sending out links to
            # patients and their treating clinicans. For this end, the idea is now that the data the treating clinician
            # fills in will be collected under another study id.
            row.replace({'\'': '"',
                         "I_LO_P_001c": "I_LO_P_001",
                         }, regex=True, inplace=True)

            if eCRF_filename == "End.csv":
                row.replace({"I-BI-C-026$": "I-BI-C-026wrong", "I-BI-C-026v": "I-BI-C-026",
                             "I-BI-C-027$": "I-BI-C-027wrong", "I-BI-C-027v": "I-BI-C-027",
                             "I_MA_C_011$": "I_MA_C_011wrong", "I_MA_C_011v": "I_MA_C_011",
                             "I-BI-P-025$": "I-BI-P-025wrong", "I-BI-P-025v": "I-BI-P-025",
                             "I-CA-P-023$": "I-CA-P-023wrong", "I-CA-P-023v": "I-CA-P-023",
                             }, regex=True, inplace=True)

            # TODO ADD HERE MORE LOGIC TO ADAPT FOR CHANGES IN MAGANAMED_IDS DURING THE PROJECT

            # As discussed in the e-mails below, we are generating new ID code for some Edinburgh’s participant using the following format “e.g. IC-P-001_c) and under this newly generated ID, we will be collecting clinicians assessment and also any subsequent participant assessment. We will update the data management team on a monthly basis with a list of modified ID for data merging. I have attached our internal SOP for more information on this.
            # Here is our list of modified IDs for January 2024
            # Original ID	New ID	        New ID usage time point
            # I-CA-P-004	I-CA-P-004_c	T2 for clinician, T3 for participant
            # I-CA-P-006	I-CA-P-006_c	T2 for clinician, T2 for participant
            # I-CA-P-007	I-CA-P-007_c	T2 for clinician, T2 for participant
            # I-CA-P-010	I-CA-P-010_c	T3 for participant, T2 for clinician
            # I-LO-P-006	I-LO-P-006_c	T2 for participant, T2 for clinician
            # I-CA-P-008	I-CA-P-08_c	    T2 for clinician , ESM T2 for participant
            # I-CA-P-013	I-CA-P-013_c	T3 for participant, T2 for clinician
            # We will get in touch with you end of February for any new modified ID.
            # Best wishes,
            # Fatene

            # import query to insert data (row) into table
            row_data_import_query = 'INSERT INTO ' + eCRF_name_for_query + ' VALUES (\'' + "\', \'".join(
                map(str, (list(row)))) + '\')'
            # execute insert query
            cursor.execute(row_data_import_query)


def use_maganamed_data(conn, config):
    codebook_filename = r"codebook.yaml"
    maganamed_ecrf_files_path = config['localPaths']['maganamed_ecrf_files']

    # Import codebook_yaml
    codebook = read_codebook_yaml(codebook_filename)

    # get all eCRF.csv filenames in export folder
    eCRF_filenames = os.listdir(maganamed_ecrf_files_path)

    # loop through all eCRF files - perform import of data into db
    ids = pd.DataFrame()
    for eCRF_filename in eCRF_filenames:
        # only use files with .csv filetype
        if eCRF_filename.endswith('.csv'):
            # load Maganamed csv data into Python
            csv_data = pd.read_csv(maganamed_ecrf_files_path + '/' + eCRF_filename, sep=";")

            # get all participant_identifiers into ids
            if len(csv_data['participant_identifier']) > 0:
                ids = ids.append(csv_data['participant_identifier'])
                ids.rename({'participant_identifier': eCRF_filename}, inplace=True, axis=0)

            # load Maganamed csv data into SQL db
            import_maganamed_data_into_sqllite(conn, eCRF_filename, csv_data, codebook)

    # save all participant_identifiers into an excel file
    ids.to_excel("all_participant_identifiers.xlsx", index=False)


# Function to import data into DB
def import_data_into_sql_lite(conn, filename, csv_data):
    # Create cursor with SQLite db connection
    cursor = conn.cursor()
    csv_data.to_sql(filename, conn, if_exists='replace', index=False)
    cursor.close()
    print(f"Table {filename} created")


# Get all files to import them into database
def retrieve_input_files(path, connect):
    # get all .csv filenames in export folder
    for root, dirs, files in os.walk(path):
        for filename in files:
            # only use files with .csv filetype
            if filename.endswith('.csv'):
                # load momentApp csv data into Python
                csv_data = pd.read_csv(root + '/' + filename, sep=",", low_memory=False)
                # load Momentapp csv data into SQL db
                filename = filename.replace(".csv", "")
                import_data_into_sql_lite(connect, filename, csv_data)


if __name__ == '__main__':
    # Read configuration file
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # define variables
    sql_lite_database_name = config['database_name']

    # Create connection to SQLite database
    connect = create_connection(sql_lite_database_name)
    if connect is not None: print("Successful connection to SQLite database")

    file_directories = {
        'dmmh_momentapp_path': config['localPaths']['dmmh_momentapp'],
        'maganamed_path': config['localPaths']['maganamed_ecrf_files'],
        'movisens_ESM_path': config['localPaths']['movisens_esm'],
        'movisens_sensing_path': config['localPaths']['movisens_sensing']
    }

    key = list(file_directories.keys())

    for key, data_directory in file_directories.items():
        print("Obtaining data from " + key)
        retrieve_input_files(data_directory, connect)

    if connect:
        # Finally commit db transaction/queries and close db connection
        connect.commit()
        connect.close()
