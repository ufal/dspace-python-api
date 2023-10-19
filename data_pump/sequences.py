import os
import logging
import time

import psycopg2
import const

_this_dir = os.path.dirname(os.path.abspath(__file__))
_temp_dir = os.path.join(_this_dir, "..", "temp-files")

# create temp dir if needed
if not os.path.exists(_temp_dir):
    os.makedirs(_temp_dir, exist_ok=True)

# standard console output
_logger = logging.getLogger("migrate_sequences")

# testing output to file
file_path = os.path.join(_temp_dir, "__failed_sequences.txt")
file_handler = logging.FileHandler(file_path, mode="w")
file_handler.setLevel(logging.ERROR)

_logger_test = logging.getLogger("test_migrated_sequences")
_logger_test.setLevel(logging.ERROR)
_logger_test.addHandler(file_handler)

def migrate_sequences():
    """
        Migrate sequences from clarin 5 database to clarin 7 database.
    """
    _logger.info("Sequence migration started.")

    # create database connection
    c5_dspace_conn = connect_to_db(database=const.CLARIN_DSPACE_NAME,
                                   host=const.CLARIN_DSPACE_HOST,
                                   user=const.CLARIN_DSPACE_USER,
                                   password=const.CLARIN_DSPACE_PASSWORD)

    c5_utilities_conn = connect_to_db(database=const.CLARIN_UTILITIES_NAME,
                                      host=const.CLARIN_UTILITIES_HOST,
                                      user=const.CLARIN_UTILITIES_USER,
                                      password=const.CLARIN_UTILITIES_PASSWORD)

    c7_dspace = connect_to_db(database=const.CLARIN_DSPACE_7_NAME,
                              host=const.CLARIN_DSPACE_7_HOST,
                              port=const.CLARIN_DSPACE_7_PORT,
                              user=const.CLARIN_DSPACE_7_USER,
                              password=const.CLARIN_DSPACE_7_PASSWORD)

    # get all sequences from clarin-dspace database
    cursor_c5_dspace = c5_dspace_conn.cursor()
    cursor_c5_dspace.execute("SELECT * FROM information_schema.sequences")
    c5_dspace_seq = cursor_c5_dspace.fetchall()

    # Do not import `clarin-utilities` sequences because of this issue:
    # https://github.com/dataquest-dev/dspace-python-api/issues/114

    # # get all sequences from clarin-utilities database
    # cursor_c5_utilities = c5_utilities_conn.cursor()
    # cursor_c5_utilities.execute("SELECT * FROM information_schema.sequences")
    # c5_utilities_seq = cursor_c5_utilities.fetchall()
    #
    # # join all clarin5 sequences into one list as clarin 7 only has one database for sequences
    clarin5_all_seq = c5_dspace_seq

    cursor_c7_dspace = c7_dspace.cursor()
    cursor_c7_dspace.execute("SELECT * FROM information_schema.sequences")
    c7_dspace_seq = cursor_c7_dspace.fetchall()
    c7_dspace_seq_names = [seq[2] for seq in c7_dspace_seq]

    name_idx = 2
    db_idx = 0

    # check if all sequences from clarin 5 are already present in clarin 7
    failed_seq = []
    for c5_seq in clarin5_all_seq:

        c5_seq_name = c5_seq[name_idx]
        seq_db = c5_seq[db_idx]

        if c5_seq_name not in c7_dspace_seq_names:
            continue

        # use cursor according to database to which sequence belongs
        if seq_db == "clarin-dspace":
            cursor = cursor_c5_dspace
        # else:
        #     cursor = cursor_c5_utilities

        # get current value of given sequence
        cursor.execute(f"SELECT last_value FROM {c5_seq_name}")
        c5_seq_val = cursor.fetchone()[0]

        # set value of the sequence in clarin 7 dspace database
        cursor_c7_dspace.execute(f"SELECT setval('{c5_seq_name}', {c5_seq_val})")
        c7_dspace.commit()

        # check value of the sequence in clarin7 database
        test_seq_value(cursor_c7_dspace, c5_seq_name, c5_seq_val)

    _logger.info("Sequence migration is complete.")


def connect_to_db(database: str, user: str, password: str, host="localhost", port=5432, max_attempt=5, conn_delay=2):
    """
        Try to connect to database with given credential in fixed number of attempt.
        Throws ConnectionError exception if fails to do so.
    """
    for conn_ctr in range(max_attempt):
        conn = psycopg2.connect(database=database,
                                host=host,
                                port=port,
                                user=user,
                                password=password)

        if conn.closed == 0:
            _logger.debug(f"Connection to {database} successful.")
            return conn

        _logger.warning(f"Connection to {database} failed. Next attempt [no. {conn_ctr}] in {conn_delay} seconds.")
        time.sleep(conn_delay)

    raise ConnectionError(f"Connection to {database} could not be established in {max_attempt} attempts.")


def test_seq_value(cursor: psycopg2.extensions.cursor, seq_name: str, expected_val: int):
    cursor.execute(f"SELECT last_value FROM {seq_name}")
    seq_val = cursor.fetchone()[0]

    if seq_val != expected_val:
        _logger_test.error(f"{seq_name}   --> [{seq_val}] does not match expected [{expected_val}].")
