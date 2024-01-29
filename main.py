import pandas as pd
import sqlite3
from sqlite3 import Error
import yaml
import os


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn


def read_codebook_yaml(codebook_filename):
    """ read codebook_yaml file in """
    # Read configuration file
    with open(codebook_filename, "r") as f:
        codebook = yaml.load(f, Loader=yaml.FullLoader)
    return codebook


def get_target_eCRF(Dictionary, eCRF_filename):
    """ scan through yaml file to find eCRF target and return index"""
    for key1 in Dictionary.keys():
        item = {key1: key2 for key2 in Dictionary[key1].keys() if eCRF_filename in Dictionary[key1][key2].values()}
        if len(item) > 0:
            return item


def create_data_types_string_for_query(codebook, eCRF_filename):
    """" create string for sql query to create table and define columns with data types """
    print(eCRF_filename)

    # get target eCRF index in codebook.yaml file
    eCRF_search_result = get_target_eCRF(codebook, eCRF_filename)['eCRFs']

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

            # import query to insert data (row) into table
            row_data_import_query = 'INSERT INTO ' + eCRF_name_for_query + ' VALUES (\'' + "\', \'".join(map(str, (list(row)))) + '\')'
            # execute insert query
            cursor.execute(row_data_import_query)


def import_momentapp_data_into_sqllite(conn, site_filename, csv_data):
    # Create cursor with SQLite db connection
    cursor = conn.cursor()

    # reformat eCRF filename to allow the usage as db table name
    site_name_for_query = site_filename.rsplit('.csv')[0].replace('-', '_').replace('(', '').replace(')', '')

    csv_data_transformed = csv_data.transpose()
    for index, row in csv_data.iterrows():
        table_columns = row.keys()
        table_column_string = ""
        for table_column in table_columns:
            table_column_string = table_column_string + table_column + " TEXT,"
        table_column_string = table_column_string[:-1] + ', PRIMARY KEY (pseudonym)'


    # create final query string
    create_table_query = 'CREATE TABLE IF NOT EXISTS ' + site_name_for_query + ' (' + table_column_string + ')'
    # execute SQL query to create table
    cursor.execute(create_table_query)


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

# Still under testing - CURRENTLY NOT WORKING!
def use_momentapp_data(conn, config):
    momentapp_sites_files_path = config['localPaths']['momentapp_sites_files_path']

    # get all .csv filenames in export folder
    site_filenames = os.listdir(momentapp_sites_files_path)

    # loop through all eCRF files - perform import of data into db
    for site_filename in site_filenames:
        # only use files with .csv filetype
        if site_filename.endswith('.csv'):
            # load momentApp csv data into Python
            csv_data = pd.read_csv(momentapp_sites_files_path + '/' + site_filename, sep=",")

            # load Maganamed csv data into SQL db
            import_momentapp_data_into_sqllite(conn, site_filename, csv_data)

    # save all participant_identifiers into an excel file


if __name__ == '__main__':
    # Read configuration file
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # define variables
    sql_lite_database_name = config['database_name']

    # Create connection to SQLite database
    conn = create_connection(sql_lite_database_name)

    use_maganamed_data(conn, config)
    #use_momentapp_data(conn, config)

    if conn:
        # Finally commit db transaction/queries and close db connection
        conn.commit()
        conn.close()
