import argparse
import logging
import os

import psycopg2
import json


def get_data_as_json(database, host, db_user, db_password):
    # create database connection
    conn = psycopg2.connect(database=database,
                            host=host,
                            user=db_user,
                            password=db_password)
    logging.info("Connection was successful!")

    cursor = conn.cursor()
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE is_insertable_into = "
        "'YES' AND table_schema = 'public'")
    # list of tuples
    table_name = cursor.fetchall()
    logging.info("Processing...")
    for name_t in table_name:
        # access to 0. position, because name_t is tuple
        name = name_t[0]
        j_name = os.path.join("data", "name" + ".json")
        with open(j_name, 'w', encoding='utf-8') as j:
            cursor.execute("SELECT json_agg(row_to_json(t)) FROM \"{}\" t".format(name))
            # access to 0. position, because the fetchone returns tuple
            created_json = json.dumps(cursor.fetchone()[0])
            j.write(created_json)
    logging.info("Data was successfully exported!")
    conn.close()
    logging.info("Disconnect from database!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process database connection')
    parser.add_argument('--database', help='database name',
                        required=True, type=str)
    parser.add_argument('--host', help='type of host', required=True, type=str)
    parser.add_argument('--user', help='database user', required=True, type=str)
    parser.add_argument('--password', help='database password',
                        required=True, type=str)
    args = parser.parse_args()
    get_data_as_json(args.database, args.host, args.user, args.password)
