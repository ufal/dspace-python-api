import os
import logging
import time

import psycopg2

_logger = logging.getLogger("migrate_sequences")

_this_dir = os.path.dirname(os.path.abspath(__file__))

settings = {
    "host": "localhost",
    "user": "postgres",
    "password": "aaa"
}


def migrate_sequences():
    """
        Migrate sequences from clarin 5 database to clarin 7 database.
    """
    _logger.info("Sequence migration started.")

    # create database connection
    c5_dspace_conn = connect_to_db(database="clarin-dspace",
                                   host=settings["host"],
                                   user=settings["user"],
                                   password=settings["password"])

    c5_utilities_conn = connect_to_db(database="clarin-utilities",
                                      host=settings["host"],
                                      user=settings["user"],
                                      password=settings["password"])

    c7_dspace = connect_to_db(database="dspace",
                              host="localhost",
                              port=5430,
                              user="dspace",
                              password="dspace")

    # get all sequences from clarin-dspace database
    cursor_c5_dspace = c5_dspace_conn.cursor()
    cursor_c5_dspace.execute("SELECT * FROM information_schema.sequences")
    c5_dspace_seq = cursor_c5_dspace.fetchall()

    # get all sequences from clarin-utilities database
    cursor_c5_utilities = c5_utilities_conn.cursor()
    cursor_c5_utilities.execute("SELECT * FROM information_schema.sequences")
    c5_utilities_seq = cursor_c5_utilities.fetchall()

    # join all clarin5 sequences into one list as clarin 7 only has one database for sequences
    clarin5_all_seq = c5_dspace_seq + c5_utilities_seq

    cursor_c7_dspace = c7_dspace.cursor()
    cursor_c7_dspace.execute("SELECT * FROM information_schema.sequences")
    c7_dspace_seq = cursor_c7_dspace.fetchall()
    c7_dspace_seq_names = [seq[2] for seq in c7_dspace_seq]

    name_idx = 2
    db_idx = 0

    # check if all sequences from clarin 5 are already present in clarin 7
    missing_seq = []
    for c5_seq in clarin5_all_seq:

        c5_seq_name = c5_seq[name_idx]
        seq_db = c5_seq[db_idx]

        if c5_seq_name in c7_dspace_seq_names:
            # use cursor according to database to which sequence belongs
            if seq_db == "clarin-dspace":
                cursor = cursor_c5_dspace
            else:
                cursor = cursor_c5_utilities

            # get current value of given sequence
            cursor.execute(f"SELECT last_value FROM {c5_seq_name}")
            c5_seq_val = cursor.fetchone()[0]

            # set value of the sequence in clarin 7 dspace database
            cursor_c7_dspace.execute(f"SELECT setval('{c5_seq_name}', {c5_seq_val})")
            c7_dspace.commit()

            # check value of the sequence in clarin7 database
            cursor_c7_dspace.execute(f"SELECT last_value FROM {c5_seq_name}")
            c7_seq_val = cursor_c7_dspace.fetchone()[0]

            if c5_seq_val != c7_seq_val:
                _logger.error(f"Sequence [{c5_seq_val}] value [{c7_seq_val}] in clarin7 "
                              f"does not match expected value [{c5_seq_val}] in clarin5.")

        else:
            # add to missing sequences list which will be exported to file later
            missing_seq.append(c5_seq_name)

    # export missing sequences to file
    temp_dir = os.path.join(_this_dir, "..", "temp-files")
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir, exist_ok=True)

    file_path = os.path.join(temp_dir, "__missing_sequences.txt")

    with open(file_path, mode="w", encoding="utf-8") as file:
        file.write("\n".join(missing_seq))

    _logger.info("Sequence migration is complete. Missing sequences are in root/temp/missing_sequences.txt")


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
