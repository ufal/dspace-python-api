import psycopg2
import json
import sys

def get_data_as_json(database, host, db_user, db_password):

#create database connection
    conn = psycopg2.connect(database=database,
                            host=host,
                            user=db_user,
                            password=db_password)
    print("Connection was successful!")

    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE is_insertable_into = 'YES' AND table_schema = 'public'")
    #list of tuples
    table_name = cursor.fetchall()
    print("Processing...")
    for name_t in table_name:
        #access to 0. position, because name_t is tuple
        name = name_t[0]
        fp_name = 'data/' + name + '.json'
        fp = open(fp_name, 'w')
        cursor.execute("SELECT json_agg(row_to_json(t)) FROM {} t".format(name))
        #access to 0. position, because the fetchone returns tuple
        fp.write((json.dumps(cursor.fetchone()[0])))
        fp.close()
    print("Data was successfully exported!")
    conn.close()
    print("Disconnect from database!")

if __name__== "__main__":
    get_data_as_json(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
